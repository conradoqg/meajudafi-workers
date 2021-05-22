ALTER TABLE private.processed_url
    ADD COLUMN last_modified text;

COMMENT ON COLUMN private.processed_url.last_modified
    IS 'ETag da URL';

COMMENT ON COLUMN private.processed_url.etag
    IS 'Last-Modified da URL';