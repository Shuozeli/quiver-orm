use crate::ast::Span;
use quiver_error::QuiverError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Config,
    Generate,
    Enum,
    Model,
    // Identifiers and literals
    Ident(String),
    StringLit(String),
    IntLit(i64),
    FloatLit(f64),
    True,
    False,
    // Symbols
    LBrace,   // {
    RBrace,   // }
    LParen,   // (
    RParen,   // )
    LBracket, // [
    RBracket, // ]
    LAngle,   // <
    RAngle,   // >
    Comma,    // ,
    Colon,    // :
    Question, // ?
    At,       // @
    AtAt,     // @@
    // End
    Eof,
}

#[derive(Debug, Clone)]
pub struct SpannedToken {
    pub token: Token,
    pub span: Span,
}

/// Maximum schema source size (10 MB).
const MAX_SCHEMA_SIZE: usize = 10 * 1024 * 1024;
/// Maximum number of tokens before we bail out.
const MAX_TOKENS: usize = 1_000_000;
/// Maximum identifier length.
const MAX_IDENT_LENGTH: usize = 256;

pub struct Lexer<'a> {
    source: &'a [u8],
    pos: usize,
    line: usize,
    column: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(source: &'a str) -> Self {
        Self {
            source: source.as_bytes(),
            pos: 0,
            line: 1,
            column: 1,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<SpannedToken>, QuiverError> {
        if self.source.len() > MAX_SCHEMA_SIZE {
            return Err(QuiverError::Parse {
                line: 1,
                column: 1,
                message: format!(
                    "schema source exceeds maximum size ({} bytes, limit {})",
                    self.source.len(),
                    MAX_SCHEMA_SIZE
                ),
            });
        }

        let mut tokens = Vec::new();
        loop {
            if tokens.len() >= MAX_TOKENS {
                return Err(QuiverError::Parse {
                    line: self.line,
                    column: self.column,
                    message: format!("token count exceeds limit ({})", MAX_TOKENS),
                });
            }
            let tok = self.next_token()?;
            let is_eof = tok.token == Token::Eof;
            tokens.push(tok);
            if is_eof {
                break;
            }
        }
        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<SpannedToken, QuiverError> {
        self.skip_whitespace_and_comments();

        if self.pos >= self.source.len() {
            return Ok(SpannedToken {
                token: Token::Eof,
                span: self.span(),
            });
        }

        let span = self.span();
        let ch = self.source[self.pos];

        match ch {
            b'{' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::LBrace,
                    span,
                })
            }
            b'}' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::RBrace,
                    span,
                })
            }
            b'(' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::LParen,
                    span,
                })
            }
            b')' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::RParen,
                    span,
                })
            }
            b'[' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::LBracket,
                    span,
                })
            }
            b']' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::RBracket,
                    span,
                })
            }
            b'<' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::LAngle,
                    span,
                })
            }
            b'>' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::RAngle,
                    span,
                })
            }
            b',' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::Comma,
                    span,
                })
            }
            b':' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::Colon,
                    span,
                })
            }
            b'?' => {
                self.advance();
                Ok(SpannedToken {
                    token: Token::Question,
                    span,
                })
            }
            b'@' => {
                self.advance();
                if self.pos < self.source.len() && self.source[self.pos] == b'@' {
                    self.advance();
                    Ok(SpannedToken {
                        token: Token::AtAt,
                        span,
                    })
                } else {
                    Ok(SpannedToken {
                        token: Token::At,
                        span,
                    })
                }
            }
            b'"' => self.read_string(span),
            b'-' | b'0'..=b'9' => self.read_number(span),
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => self.read_ident(span),
            _ => Err(QuiverError::Parse {
                line: span.line,
                column: span.column,
                message: format!("unexpected character '{}'", ch as char),
            }),
        }
    }

    fn read_string(&mut self, span: Span) -> Result<SpannedToken, QuiverError> {
        self.advance(); // skip opening "
        let mut value = String::new();
        while self.pos < self.source.len() && self.source[self.pos] != b'"' {
            if self.source[self.pos] == b'\\' {
                self.advance(); // skip backslash
                if self.pos >= self.source.len() {
                    return Err(QuiverError::Parse {
                        line: span.line,
                        column: span.column,
                        message: "unterminated escape sequence in string".into(),
                    });
                }
                match self.source[self.pos] {
                    b'\\' => value.push('\\'),
                    b'"' => value.push('"'),
                    b'n' => value.push('\n'),
                    b't' => value.push('\t'),
                    b'r' => value.push('\r'),
                    other => {
                        return Err(QuiverError::Parse {
                            line: self.line,
                            column: self.column,
                            message: format!("unknown escape sequence '\\{}'", other as char),
                        });
                    }
                }
                self.advance();
            } else {
                value.push(self.source[self.pos] as char);
                self.advance();
            }
        }
        if self.pos >= self.source.len() {
            return Err(QuiverError::Parse {
                line: span.line,
                column: span.column,
                message: "unterminated string literal".into(),
            });
        }
        self.advance(); // skip closing "
        Ok(SpannedToken {
            token: Token::StringLit(value),
            span,
        })
    }

    fn read_number(&mut self, span: Span) -> Result<SpannedToken, QuiverError> {
        let start = self.pos;
        if self.pos < self.source.len() && self.source[self.pos] == b'-' {
            self.advance();
        }
        while self.pos < self.source.len() && self.source[self.pos].is_ascii_digit() {
            self.advance();
        }
        let mut is_float = false;
        if self.pos < self.source.len() && self.source[self.pos] == b'.' {
            is_float = true;
            self.advance();
            while self.pos < self.source.len() && self.source[self.pos].is_ascii_digit() {
                self.advance();
            }
        }
        let text =
            std::str::from_utf8(&self.source[start..self.pos]).map_err(|_| QuiverError::Parse {
                line: span.line,
                column: span.column,
                message: "invalid UTF-8 in number literal".into(),
            })?;
        if is_float {
            let val: f64 = text.parse().map_err(|_| QuiverError::Parse {
                line: span.line,
                column: span.column,
                message: format!("invalid float literal '{text}'"),
            })?;
            Ok(SpannedToken {
                token: Token::FloatLit(val),
                span,
            })
        } else {
            let val: i64 = text.parse().map_err(|_| QuiverError::Parse {
                line: span.line,
                column: span.column,
                message: format!("invalid integer literal '{text}'"),
            })?;
            Ok(SpannedToken {
                token: Token::IntLit(val),
                span,
            })
        }
    }

    fn read_ident(&mut self, span: Span) -> Result<SpannedToken, QuiverError> {
        let start = self.pos;
        while self.pos < self.source.len()
            && (self.source[self.pos].is_ascii_alphanumeric() || self.source[self.pos] == b'_')
        {
            self.advance();
        }
        let len = self.pos - start;
        if len > MAX_IDENT_LENGTH {
            return Err(QuiverError::Parse {
                line: span.line,
                column: span.column,
                message: format!(
                    "identifier exceeds maximum length ({len}, limit {MAX_IDENT_LENGTH})"
                ),
            });
        }
        let text =
            std::str::from_utf8(&self.source[start..self.pos]).map_err(|_| QuiverError::Parse {
                line: span.line,
                column: span.column,
                message: "invalid UTF-8 in identifier".into(),
            })?;
        let token = match text {
            "config" => Token::Config,
            "generate" => Token::Generate,
            "enum" => Token::Enum,
            "model" => Token::Model,
            "true" => Token::True,
            "false" => Token::False,
            _ => Token::Ident(text.to_string()),
        };
        Ok(SpannedToken { token, span })
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while self.pos < self.source.len() && self.source[self.pos].is_ascii_whitespace() {
                self.advance();
            }
            // Skip // comments
            if self.pos + 1 < self.source.len()
                && self.source[self.pos] == b'/'
                && self.source[self.pos + 1] == b'/'
            {
                while self.pos < self.source.len() && self.source[self.pos] != b'\n' {
                    self.advance();
                }
                continue;
            }
            break;
        }
    }

    fn advance(&mut self) {
        if self.pos < self.source.len() {
            if self.source[self.pos] == b'\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
            self.pos += 1;
        }
    }

    fn span(&self) -> Span {
        Span {
            line: self.line,
            column: self.column,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(input: &str) -> Vec<Token> {
        Lexer::new(input)
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn keywords() {
        assert_eq!(
            lex("config generate enum model"),
            vec![
                Token::Config,
                Token::Generate,
                Token::Enum,
                Token::Model,
                Token::Eof
            ]
        );
    }

    #[test]
    fn symbols() {
        assert_eq!(
            lex("{ } ( ) [ ] < > , : ? @ @@"),
            vec![
                Token::LBrace,
                Token::RBrace,
                Token::LParen,
                Token::RParen,
                Token::LBracket,
                Token::RBracket,
                Token::LAngle,
                Token::RAngle,
                Token::Comma,
                Token::Colon,
                Token::Question,
                Token::At,
                Token::AtAt,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn string_literal() {
        assert_eq!(
            lex(r#""hello world""#),
            vec![Token::StringLit("hello world".into()), Token::Eof]
        );
    }

    #[test]
    fn number_literals() {
        assert_eq!(
            lex("42 -7 2.72"),
            vec![
                Token::IntLit(42),
                Token::IntLit(-7),
                Token::FloatLit(2.72),
                Token::Eof
            ]
        );
    }

    #[test]
    fn booleans() {
        assert_eq!(
            lex("true false"),
            vec![Token::True, Token::False, Token::Eof]
        );
    }

    #[test]
    fn identifiers() {
        assert_eq!(
            lex("Int32 Utf8 User my_field"),
            vec![
                Token::Ident("Int32".into()),
                Token::Ident("Utf8".into()),
                Token::Ident("User".into()),
                Token::Ident("my_field".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn comments_skipped() {
        assert_eq!(
            lex("config // this is a comment\nmodel"),
            vec![Token::Config, Token::Model, Token::Eof]
        );
    }

    #[test]
    fn model_field_line() {
        assert_eq!(
            lex("id Int32 @id @autoincrement"),
            vec![
                Token::Ident("id".into()),
                Token::Ident("Int32".into()),
                Token::At,
                Token::Ident("id".into()),
                Token::At,
                Token::Ident("autoincrement".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn nullable_type() {
        assert_eq!(
            lex("Utf8?"),
            vec![Token::Ident("Utf8".into()), Token::Question, Token::Eof]
        );
    }

    #[test]
    fn generic_type() {
        assert_eq!(
            lex("List<Utf8>"),
            vec![
                Token::Ident("List".into()),
                Token::LAngle,
                Token::Ident("Utf8".into()),
                Token::RAngle,
                Token::Eof,
            ]
        );
    }
}
