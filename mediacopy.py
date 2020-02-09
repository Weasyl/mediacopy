import base64
import enum
import hashlib
import os
import secrets
import struct
import time
from dataclasses import dataclass
from io import BytesIO
from typing import Optional

import boto3
from sqlalchemy import create_engine


MEDIA_ROOT = os.environb[b"MEDIA_ROOT"]
INTERVAL = int(os.environ.get("INTERVAL", "20"))
SQLALCHEMY_URL = os.environ["SQLALCHEMY_URL"]
BUCKET_NAME = os.environ["BUCKET_NAME"]

YEAR_SECONDS = 365 * 24 * 3600
FILE_KEY_LENGTH = 16
CHUNK_SIZE = 8192


_IMAGE_MEDIA_TYPES = ("image/png", "image/jpeg", "image/gif", "image/webp")
_IMAGE_EXTENSIONS = (".png", ".jpg", ".gif", ".webp")


@enum.unique
class ImageContentType(enum.Enum):
    """
    The format a static image is stored in.
    """

    PNG = 1
    JPEG = 2
    GIF_STATIC = 3
    WEBP_STATIC = 4  # weasyl-old has no lossiness distinction

    def serialize_to(self, f):
        f.write(bytes([self.value]))

    @property
    def media_type(self):
        return _IMAGE_MEDIA_TYPES[self.value - 1]

    @property
    def extension(self):
        return _IMAGE_EXTENSIONS[self.value - 1]


@dataclass(eq=False, frozen=True)
class ImageRepresentation:
    content_type: ImageContentType
    file_key: bytes
    width: int
    height: int

    def __init__(self, *, content_type, file_key, width, height):
        if not (1 <= width <= 16383):
            raise ValueError(f"Image width out of range: {width}")

        if not (1 <= height <= 16383):
            raise ValueError(f"Image height out of range: {height}")

        if len(file_key) != FILE_KEY_LENGTH:
            raise ValueError(f"Invalid file key: {file_key!r}")

        object.__setattr__(self, "content_type", content_type)
        object.__setattr__(self, "file_key", file_key)
        object.__setattr__(self, "width", width)
        object.__setattr__(self, "height", height)

    def serialize_to(self, f):
        self.content_type.serialize_to(f)
        f.write(self.file_key)
        f.write(struct.pack(">HH", self.width, self.height))


@dataclass(eq=False, frozen=True)
class ImageRepresentations:
    """
    Representations (e.g. crops/resizes) of a static image.

    A version of ImageRepresentations for weasyl-old, where the format is
    part of the representation name.

    - If `cover` is None, it falls back on `original`.
    - If `thumbnail_custom` is None, there is no custom thumbnail.
    - If `thumbnail_generated` is None, it falls back on `cover`.
    - If `thumbnail_generated_webp` is None, it falls back on `thumbnail_generated`.
    """

    original: ImageRepresentation
    cover: ImageRepresentation
    thumbnail_custom: Optional[ImageRepresentation]
    thumbnail_generated: ImageRepresentation
    thumbnail_generated_webp: Optional[ImageRepresentation]

    def __init__(
        self, *,
        original,
        cover,
        thumbnail_custom,
        thumbnail_generated,
        thumbnail_generated_webp,
    ):
        if cover is None:
            cover = original

        if thumbnail_generated is None:
            thumbnail_generated = cover

        object.__setattr__(self, 'original', original)
        object.__setattr__(self, 'cover', cover)
        object.__setattr__(self, 'thumbnail_custom', thumbnail_custom)
        object.__setattr__(self, 'thumbnail_generated', thumbnail_generated)
        object.__setattr__(self, 'thumbnail_generated_webp', thumbnail_generated_webp)

    def serialize_to(self, f):
        included_mask = (
            (self.cover is not self.original)
            | (self.thumbnail_generated is not self.cover) << 1
            | (self.thumbnail_custom is not None) << 2
            # 1 << 3: room for original_webp
            # 1 << 4: room for cover_webp
            | (self.thumbnail_generated_webp is not None) << 5
        )

        f.write(bytes([included_mask]))

        self.original.serialize_to(f)

        if self.cover is not self.original:
            self.cover.serialize_to(f)

        if self.thumbnail_generated is not self.cover:
            self.thumbnail_generated.serialize_to(f)

        if self.thumbnail_custom is not None:
            self.thumbnail_custom.serialize_to(f)

        if self.thumbnail_generated_webp is not None:
            self.thumbnail_generated_webp.serialize_to(f)


# A mapping from a media file_type representing a static image to its ImageContentType.
IMAGE_CONTENT_TYPES = {
    "gif": ImageContentType.GIF_STATIC,
    "jpg": ImageContentType.JPEG,
    "png": ImageContentType.PNG,
    "webp": ImageContentType.WEBP_STATIC,
}


def rep_from_media(media):
    """
    Get an ImageRepresentation from a media dict representing a static image.
    """
    return ImageRepresentation(
        content_type=IMAGE_CONTENT_TYPES[media["file_type"]],
        file_key=secrets.token_bytes(FILE_KEY_LENGTH),
        width=int(media["attributes"]["width"]),
        height=int(media["attributes"]["height"]),
    )


def get_bucket_key(rep):
    base = base64.urlsafe_b64encode(rep.file_key).rstrip(b"=").decode("ascii")
    return base + rep.content_type.extension


def lookup_by(iterable, key_func):
    result = {}

    for x in iterable:
        key = key_func(x)

        if key in result:
            raise ValueError(f"Unexpected duplicate key {key!r}")

        result[key] = x

    return result


def subdict(d, keys):
    return {k: d[k] for k in keys if k in d}


def map_values(func, d):
    return {k: func(v) for k, v in d.items()}


def create_reps(bucket, row):
    links = subdict(
        lookup_by(row.links, lambda link: link["link_type"]),
        ["submission", "cover", "thumbnail-custom", "thumbnail-generated", "thumbnail-generated-webp"],
    )

    media_by_mediaid = {
        link["mediaid"]: subdict(link, ["sha256", "file_type", "attributes"])
        for link in links.values()}

    # create a representation for each distinct media item
    rep_by_mediaid = map_values(rep_from_media, media_by_mediaid)

    def get_rep(link_type, missing_ok=False):
        try:
            media = links[link_type]
        except KeyError as e:
            if missing_ok:
                return None
            else:
                raise Exception(f"Required key not found in {links!r}") from e

        return rep_by_mediaid[media["mediaid"]]

    reps = ImageRepresentations(
        original=get_rep("submission"),
        cover=get_rep("cover"),
        thumbnail_custom=get_rep("thumbnail-custom", missing_ok=True),
        thumbnail_generated=get_rep("thumbnail-generated"),
        thumbnail_generated_webp=get_rep("thumbnail-generated-webp", missing_ok=True),
    )

    # upload each created representation to bucket
    for mediaid, rep in rep_by_mediaid.items():
        media = media_by_mediaid[mediaid]

        file_path = os.path.join(
            MEDIA_ROOT,
            media["sha256"][0:2].encode("ascii"),
            media["sha256"][2:4].encode("ascii"),
            media["sha256"][4:6].encode("ascii"),
            (media["sha256"] + "." + media["file_type"]).encode("ascii"),
        )

        content_length = 0
        md5 = hashlib.md5()
        sha256 = hashlib.sha256()

        with open(file_path, "rb") as media_file:
            for chunk in iter(lambda: media_file.read(CHUNK_SIZE), b""):
                md5.update(chunk)
                sha256.update(chunk)
                content_length += len(chunk)

            if sha256.hexdigest() != media["sha256"]:
                raise Exception(f"SHA-256 mismatch: got {sha256.hexdigest()!r} for {file_path!r}")

            media_file.seek(0)

            bucket.put_object(
                Body=media_file,
                CacheControl=f"public, max-age={YEAR_SECONDS}, immutable",
                ContentMD5=base64.b64encode(md5.digest()).decode("ascii"),
                ContentLength=content_length,
                ContentType=rep.content_type.media_type,
                Key=get_bucket_key(rep),
            )

    return reps, len(rep_by_mediaid)


def main():
    """
    Every INTERVAL seconds, upload the newest non-uploaded submission to S3.

    Exits on failure at any point, potentially leaving orphaned objects behind in the bucket.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(BUCKET_NAME)
    db = create_engine(SQLALCHEMY_URL)

    while True:
        row = db.execute(
            "SELECT submission.submitid, CASE WHEN min(submission_media_links.submitid) IS NULL THEN NULL ELSE array_agg(jsonb_build_object('link_type', link_type, 'mediaid', media.mediaid, 'sha256', media.sha256, 'file_type', media.file_type, 'attributes', media.attributes)) END AS links"
            " FROM submission"
            " LEFT JOIN submission_media_links USING (submitid)"
            " LEFT JOIN media USING (mediaid)"
            " WHERE subtype BETWEEN 1000 AND 1999"
            " AND image_representations IS NULL"
            " GROUP BY submission.submitid"
            " ORDER BY submission.submitid DESC LIMIT 1"
        ).first()

        if row is None:
            print("No rows remaining")
            break

        print(f"submission {row.submitid}: uploading representations")

        reps, count = create_reps(bucket, row)

        with BytesIO() as reps_buffer:
            reps.serialize_to(reps_buffer)
            reps_bytes = reps_buffer.getvalue()

        db.execute(
            "UPDATE submission SET image_representations = %(reps_bytes)s WHERE submitid = %(submitid)s",
            reps_bytes=reps_bytes,
            submitid=row.submitid,
        )

        print(f"submission {row.submitid}: uploaded {count} representations")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
