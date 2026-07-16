#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfficialParamsKind {
    OpenAiCodexCli,
    DreaminaCli,
    XaiImage,
    XaiVideo,
    VolcengineJimengImage,
    VolcengineJimengVideo,
    BytePlusSeedanceVideo,
}

impl OfficialParamsKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAiCodexCli => "openai_codex_cli",
            Self::DreaminaCli => "dreamina_cli",
            Self::XaiImage => "xai_image",
            Self::XaiVideo => "xai_video",
            Self::VolcengineJimengImage => "volcengine_jimeng_image",
            Self::VolcengineJimengVideo => "volcengine_jimeng_video",
            Self::BytePlusSeedanceVideo => "byteplus_seedance_video",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfficialParamsContract {
    pub kind: OfficialParamsKind,
    pub schema_id: &'static str,
    pub passthrough_allowed: bool,
}
