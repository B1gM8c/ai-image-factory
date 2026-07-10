use async_trait::async_trait;

use crate::ImageGatewayError;

#[derive(Clone, Debug)]
pub struct GenerationJob {
    pub request_id: String,
    pub model: String,
    pub prompt: String,
    pub n: u32,
    pub size: String,
    pub quality: String,
    pub output_format: String,
    pub output_compression: Option<u8>,
    pub background: String,
    pub stream: bool,
    pub partial_images: u32,
}

#[derive(Clone, Debug)]
pub struct EditJob {
    pub request_id: String,
    pub model: String,
    pub prompt: String,
    pub images: Vec<InputImage>,
    pub mask: Option<InputImage>,
    pub n: u32,
    pub size: String,
    pub quality: String,
    pub output_format: String,
    pub output_compression: Option<u8>,
    pub background: String,
    pub stream: bool,
    pub partial_images: u32,
}

#[derive(Clone, Debug)]
pub struct InputImage {
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct GeneratedImage {
    pub bytes: Vec<u8>,
}

#[async_trait]
pub trait ImageGenerator: Send + Sync + 'static {
    async fn generate(&self, job: GenerationJob) -> Result<Vec<GeneratedImage>, ImageGatewayError>;

    async fn edit(&self, job: EditJob) -> Result<Vec<GeneratedImage>, ImageGatewayError>;
}
