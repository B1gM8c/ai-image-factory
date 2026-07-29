ALTER TABLE project_model_rate_states
    DROP CONSTRAINT project_model_rate_states_project_id_bucket_key_fkey;

ALTER TABLE project_model_rate_states
    ADD CONSTRAINT project_model_rate_states_project_id_fkey
    FOREIGN KEY (project_id)
    REFERENCES gateway_projects(id) ON DELETE RESTRICT;

ALTER TABLE project_model_rate_admissions
    DROP CONSTRAINT project_model_rate_admissions_project_id_bucket_key_fkey;

ALTER TABLE project_model_rate_admissions
    ADD CONSTRAINT project_model_rate_admissions_project_id_fkey
    FOREIGN KEY (project_id)
    REFERENCES gateway_projects(id) ON DELETE RESTRICT;

COMMENT ON TABLE project_model_rate_states IS
    'Transactional token-bucket state for the effective project limit. The effective limit is the project override when present, otherwise the inherited platform limit.';

COMMENT ON TABLE project_model_rate_admissions IS
    'Accepted effective project/model rate admissions, idempotent by admission session. Provider failure does not refund rate tokens.';
