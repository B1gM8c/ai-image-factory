#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfficialParamsKind {
    OpenAiCodexCli,
    XaiImage,
    XaiVideo,
    VolcengineJimengImage,
    VolcengineJimengVideo,
    BytePlusSeedanceVideo,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfficialParamsContract {
    pub kind: OfficialParamsKind,
    pub schema_id: &'static str,
    pub passthrough_allowed: bool,
}
