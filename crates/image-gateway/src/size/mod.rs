#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeConstraint {
    Auto,
    Dimensions { width: u32, height: u32 },
    AspectRatio { width: u32, height: u32 },
}

pub fn parse_size_constraint(size: &str) -> Option<SizeConstraint> {
    if size == "auto" {
        return Some(SizeConstraint::Auto);
    }

    if let Some((width, height)) = size.split_once('x') {
        return Some(SizeConstraint::Dimensions {
            width: parse_positive_u32(width)?,
            height: parse_positive_u32(height)?,
        });
    }

    if let Some((width, height)) = size.split_once(':') {
        let width = parse_positive_u32(width)?;
        let height = parse_positive_u32(height)?;
        let divisor = gcd(width, height);
        return Some(SizeConstraint::AspectRatio {
            width: width / divisor,
            height: height / divisor,
        });
    }

    None
}

pub fn is_valid_gpt_image_2_size(size: &str) -> bool {
    match parse_size_constraint(size) {
        Some(SizeConstraint::Auto) => true,
        Some(SizeConstraint::Dimensions { width, height }) => {
            let pixels = width as u64 * height as u64;
            let long = width.max(height) as u64;
            let short = width.min(height) as u64;

            width % 16 == 0
                && height % 16 == 0
                && long <= 3840
                && long <= short * 3
                && (655_360..=8_294_400).contains(&pixels)
        }
        Some(SizeConstraint::AspectRatio { width, height }) => {
            let long = width.max(height) as u64;
            let short = width.min(height) as u64;
            long <= short * 3
        }
        None => false,
    }
}

pub fn gcd(mut a: u32, mut b: u32) -> u32 {
    while b != 0 {
        let remainder = a % b;
        a = b;
        b = remainder;
    }
    a.max(1)
}

fn parse_positive_u32(value: &str) -> Option<u32> {
    let parsed = value.parse::<u32>().ok()?;
    (parsed > 0).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_reduces_aspect_ratio() {
        assert_eq!(
            parse_size_constraint("32:18"),
            Some(SizeConstraint::AspectRatio {
                width: 16,
                height: 9
            })
        );
    }

    #[test]
    fn validates_flexible_aspect_ratio_sizes() {
        assert!(is_valid_gpt_image_2_size("16:9"));
        assert!(is_valid_gpt_image_2_size("4:3"));
        assert!(is_valid_gpt_image_2_size("1:1"));
        assert!(!is_valid_gpt_image_2_size("4:0"));
        assert!(!is_valid_gpt_image_2_size("100:1"));
    }

    #[test]
    fn validates_dimension_boundaries() {
        for (size, valid) in [
            ("1024x1024", true),
            ("1025x1024", false),
            ("1024x1025", false),
            ("3840x1280", true),
            ("3856x1280", false),
            ("3840x1264", false),
            ("1024x640", true),
            ("1008x640", false),
            ("2880x2880", true),
            ("2896x2880", false),
        ] {
            assert_eq!(
                is_valid_gpt_image_2_size(size),
                valid,
                "unexpected validity for {size}"
            );
        }
    }
}
