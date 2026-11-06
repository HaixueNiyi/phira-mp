use fluent::{FluentArgs, FluentError, FluentResource, bundle::FluentBundle};
use fluent_syntax::ast::Pattern;
use lru::LruCache;
use once_cell::sync::Lazy;
use std::{borrow::Cow, cell::RefCell, collections::HashMap, sync::Arc};
use tracing::error;
use unic_langid::{LanguageIdentifier, langid};

pub static LANGS: [&str; 3] = ["en-US", "zh-CN", "zh-TW"]; // this should be consistent with the macro below (BUNDLES)
pub static IDENTS: Lazy<[LanguageIdentifier; 3]> =
    Lazy::new(|| LANGS.map(|it| it.parse().unwrap()));

pub struct L10nBundles {
    inner: Vec<FluentBundle<FluentResource, intl_memoizer::concurrent::IntlLangMemoizer>>,
    map: HashMap<LanguageIdentifier, usize>,
}

static BUNDLES: Lazy<L10nBundles> = Lazy::new(|| {
    let mut map = HashMap::new();
    macro_rules! bundle {
        ($locale:literal) => {{
            map.insert(langid!($locale), map.len());
            let mut bundle = FluentBundle::new_concurrent(vec![langid!($locale)]);
            bundle
                .add_resource(
                    FluentResource::try_new(
                        include_str!(concat!(
                            env!("CARGO_MANIFEST_DIR"),
                            "/locales/",
                            $locale,
                            ".ftl"
                        ))
                        .to_owned(),
                    )
                    .unwrap(),
                )
                .unwrap();
            bundle.set_use_isolating(false);
            bundle
        }};
    }
    L10nBundles {
        inner: vec![bundle!("en-US"), bundle!("zh-CN"), bundle!("zh-TW")],
        map,
    }
});

pub struct L10nLocal {
    cache: [LruCache<&'static str, (usize, &'static Pattern<&'static str>)>; 3],
}

impl L10nLocal {
    fn new() -> Self {
        let size = 64.try_into().unwrap();
        Self {
            cache: std::array::from_fn(|_| LruCache::new(size)),
        }
    }

    fn format_with_errors<'s>(
        &mut self,
        lang: LanguageIdentifier,
        key: &'static str,
        args: Option<&'s FluentArgs<'s>>,
        errors: &mut Vec<FluentError>,
    ) -> Cow<'s, str> {
        let id = BUNDLES.map.get(&lang).copied().unwrap_or(0);
        let (id, pattern) = self.cache[id].get_or_insert(key, || {
            if let Some(value) = BUNDLES.inner[id]
                .get_message(key)
                .and_then(|msg| msg.value())
            {
                return (id, value);
            }
            if let Some(value) = BUNDLES.inner[0]
                .get_message(key)
                .and_then(|msg| msg.value())
            {
                return (0, value);
            }
            panic!("no translation found for {key} (lang={lang})");
        });
        BUNDLES.inner[*id].format_pattern(pattern, args, errors)
    }

    pub fn format<'s>(
        &mut self,
        lang: LanguageIdentifier,
        key: &'static str,
        args: Option<&'s FluentArgs<'s>>,
    ) -> Cow<'s, str> {
        let mut errors = Vec::new();
        let res = self.format_with_errors(lang, key, args, &mut errors);
        for error in errors {
            error!("message error {}: {:?}", key, error);
        }
        res
    }
}

thread_local! {
    static L10N_LOCAL: RefCell<L10nLocal> = RefCell::new(L10nLocal::new());
}

#[derive(Clone)]
pub struct Language(pub LanguageIdentifier);

impl Default for Language {
    fn default() -> Self {
        Self(IDENTS[0].clone())
    }
}

impl Language {
    pub fn format<'s>(&self, key: &'static str, args: Option<&'s FluentArgs<'s>>) -> Cow<'s, str> {
        L10N_LOCAL.with(|it| it.borrow_mut().format(self.0.clone(), key, args))
    }
}

tokio::task_local! {
    pub static LANGUAGE: Arc<Language>;
}

#[macro_export]
macro_rules! tl {
    ($key:literal) => {
        $crate::l10n::LANGUAGE.with(|l| l.format($key, None))
    };
    ($key:literal, $($attr:expr => $value:expr), *) => {
        $crate::l10n::LANGUAGE.with(|l| l.format($key, Some(&fluent::fluent_args![$($attr => $value), *])).into_owned())
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for H-5: when a user's language is a valid BCP-47 tag outside
    /// `LANGS` (`en-US`, `zh-CN`, `zh-TW`), `Language::format` falls back to `en-US`.
    #[test]
    fn l10n_unsupported_language_falls_back_to_en_us() {
        let en = Language::default()
            .format("join-room-locked", None)
            .into_owned();
        assert_eq!(en, "Room is locked");

        for tag in ["ja-JP", "ko-KR", "en", "fr-FR"] {
            let lang = Language(tag.parse().unwrap());
            assert_eq!(lang.format("join-room-locked", None), en);
        }
    }
}
