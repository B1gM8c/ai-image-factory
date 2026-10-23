-- Add the official Pro alias to the existing shared bucket without changing
-- historical aliases, route revisions, prices, or configured rate ceilings.
INSERT INTO platform_model_limit_members (
    operation_id, api_profile, public_model_id, media_kind,
    bucket_key, bucket_display_name, unit_kind,
    request_ceiling_per_minute, unit_ceiling_per_minute, created_at_ms
)
SELECT operation_id, 'volcengine-ark-images-v3',
       'doubao-seedream-5-0-pro-260628', media_kind,
       bucket_key, bucket_display_name, unit_kind,
       request_ceiling_per_minute, unit_ceiling_per_minute, created_at_ms
FROM platform_model_limit_members
WHERE operation_id = 'images.generations'
  AND api_profile = 'dreamina-cli-images-v1'
  AND public_model_id = '5.0Pro'
ON CONFLICT (operation_id, api_profile, public_model_id) DO NOTHING;
