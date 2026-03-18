"""Icecat Language Mapper - Maps local culture IDs to Icecat language IDs."""

from dataclasses import dataclass


@dataclass
class LanguageMapping:
    """Represents a mapping between local culture ID and Icecat language."""

    culture_id: str
    lang_id: int
    code: str
    short_code: str

    @property
    def lang_id_str(self) -> str:
        """Return lang_id as string for compatibility."""
        return str(self.lang_id)


# Global constant for international/fallback
GLOBAL_CULTURE_ID = "global"


class IcecatLanguageMapper:
    """
    Maps local culture IDs to Icecat language properties.

    """

    ICECAT_LANGUAGE_MAPPING: list[LanguageMapping] = [
        # International (fallback)
        LanguageMapping(culture_id=GLOBAL_CULTURE_ID, lang_id=0, code="International", short_code="INT"),
        # Main languages
        LanguageMapping(culture_id="en", lang_id=1, code="english", short_code="EN"),
        LanguageMapping(culture_id="nl", lang_id=2, code="dutch", short_code="NL"),
        LanguageMapping(culture_id="fr", lang_id=3, code="french", short_code="FR"),
        LanguageMapping(culture_id="de", lang_id=4, code="german", short_code="DE"),
        LanguageMapping(culture_id="it", lang_id=5, code="italian", short_code="IT"),
        LanguageMapping(culture_id="es", lang_id=6, code="spanish", short_code="ES"),
        LanguageMapping(culture_id="da", lang_id=7, code="danish", short_code="DK"),
        LanguageMapping(culture_id="ru", lang_id=8, code="russian", short_code="RU"),
        LanguageMapping(culture_id="en-US", lang_id=9, code="us english", short_code="US"),
        LanguageMapping(culture_id="pt-BR", lang_id=10, code="brazilian-portuguese", short_code="BR"),
        LanguageMapping(culture_id="pt", lang_id=11, code="portuguese", short_code="PT"),
        LanguageMapping(culture_id="zh", lang_id=12, code="simplified chinese", short_code="ZH"),
        # lang_id 13 (swedish) and 14 (polish) are not mapped
        LanguageMapping(culture_id="cs", lang_id=15, code="czech-republic", short_code="CZ"),
        LanguageMapping(culture_id="hu", lang_id=16, code="hungarian", short_code="HU"),
        LanguageMapping(culture_id="fi", lang_id=17, code="finnish", short_code="FI"),
        LanguageMapping(culture_id="el", lang_id=18, code="greek", short_code="EL"),
        LanguageMapping(culture_id="no", lang_id=19, code="norwegian", short_code="NO"),
        LanguageMapping(culture_id="tr", lang_id=20, code="turkish", short_code="TR"),
        LanguageMapping(culture_id="bg", lang_id=21, code="bulgarian", short_code="BG"),
        LanguageMapping(culture_id="ka", lang_id=22, code="georgian", short_code="KA"),
        LanguageMapping(culture_id="ro", lang_id=23, code="romanian", short_code="RO"),
        LanguageMapping(culture_id="sr", lang_id=24, code="serbian", short_code="SR"),
        LanguageMapping(culture_id="uk", lang_id=25, code="ukrainian", short_code="UK"),
        LanguageMapping(culture_id="ja", lang_id=26, code="japanese", short_code="JA"),
        LanguageMapping(culture_id="ca", lang_id=27, code="catalan", short_code="CA"),
        LanguageMapping(culture_id="es-AR", lang_id=28, code="argentinian-spanish", short_code="ES_AR"),
        LanguageMapping(culture_id="hr", lang_id=29, code="croatian", short_code="HR"),
        LanguageMapping(culture_id="ar", lang_id=30, code="arabic", short_code="AR"),
        LanguageMapping(culture_id="vi", lang_id=31, code="vietnamese", short_code="VI"),
        LanguageMapping(culture_id="ko", lang_id=32, code="korean", short_code="KO"),
        LanguageMapping(culture_id="mk", lang_id=33, code="macedonian", short_code="MK"),
        LanguageMapping(culture_id="sl", lang_id=34, code="slovenian", short_code="SL"),
        LanguageMapping(culture_id="en-SG", lang_id=35, code="singapore-english", short_code="EN_SG"),
        LanguageMapping(culture_id="en-ZA", lang_id=36, code="south africa-english", short_code="EN_ZA"),
        LanguageMapping(culture_id="zh-TW", lang_id=37, code="traditional chinese", short_code="ZH_TW"),
        LanguageMapping(culture_id="he", lang_id=38, code="hebrew", short_code="HE"),
        LanguageMapping(culture_id="lt", lang_id=39, code="lithuanian", short_code="LT"),
        LanguageMapping(culture_id="lv", lang_id=40, code="latvian", short_code="LV"),
        # lang_id 41 (indian-english) and 42 (swiss-german) are not mapped
        LanguageMapping(culture_id="id", lang_id=43, code="indonesian", short_code="ID"),
        LanguageMapping(culture_id="sk", lang_id=44, code="slovak", short_code="SK"),
        LanguageMapping(culture_id="fa", lang_id=45, code="persian", short_code="FA"),
        LanguageMapping(culture_id="es-MX", lang_id=46, code="mexican-spanish", short_code="ES_MX"),
        LanguageMapping(culture_id="et", lang_id=47, code="estonian", short_code="ET"),
        LanguageMapping(culture_id="de-BE", lang_id=48, code="belgian-german", short_code="DE_BE"),
        LanguageMapping(culture_id="fr-BE", lang_id=49, code="belgian-french", short_code="FR_BE"),
        LanguageMapping(culture_id="nl-BE", lang_id=50, code="belgian-dutch", short_code="NL_BE"),
        LanguageMapping(culture_id="th", lang_id=51, code="thai", short_code="TH"),
        LanguageMapping(culture_id="ru-UA", lang_id=52, code="ukrainian-russian", short_code="RU_UA"),
        LanguageMapping(culture_id="de-AT", lang_id=53, code="austrian-german", short_code="DE_AT"),
        LanguageMapping(culture_id="fr-CH", lang_id=54, code="swiss-french", short_code="FR_CH"),
        LanguageMapping(culture_id="en-NZ", lang_id=55, code="new zealand-english", short_code="EN_NZ"),
        LanguageMapping(culture_id="en-SA", lang_id=56, code="saudi arabia-english", short_code="EN_SA"),
        LanguageMapping(culture_id="en-ID", lang_id=57, code="indonesian-english", short_code="EN_ID"),
        LanguageMapping(culture_id="en-MY", lang_id=58, code="malaysian-english", short_code="EN_MY"),
        LanguageMapping(culture_id="hi", lang_id=59, code="hindi", short_code="HI"),
        LanguageMapping(culture_id="fr-CA", lang_id=60, code="canadian-french", short_code="FR_CA"),
        LanguageMapping(culture_id="te", lang_id=61, code="telugu", short_code="TE"),
        LanguageMapping(culture_id="ta", lang_id=62, code="tamil", short_code="TA"),
        LanguageMapping(culture_id="kn", lang_id=63, code="kannada", short_code="KN"),
        LanguageMapping(culture_id="en-IE", lang_id=64, code="ireland-english", short_code="EN_IE"),
        LanguageMapping(culture_id="ml", lang_id=65, code="malayalam", short_code="ML"),
        LanguageMapping(culture_id="en-AE", lang_id=66, code="uae-english", short_code="EN_AE"),
        LanguageMapping(culture_id="es-CL", lang_id=67, code="chilean-spanish", short_code="ES_CL"),
        LanguageMapping(culture_id="es-PE", lang_id=68, code="peruvian-spanish", short_code="ES_PE"),
        LanguageMapping(culture_id="es-CO", lang_id=69, code="colombian-spanish", short_code="ES_CO"),
        LanguageMapping(culture_id="mr", lang_id=70, code="marathi", short_code="MR"),
        LanguageMapping(culture_id="bn", lang_id=71, code="bengali", short_code="BN"),
        LanguageMapping(culture_id="ms", lang_id=72, code="malay", short_code="MS"),
        LanguageMapping(culture_id="en-AU", lang_id=73, code="australian-english", short_code="EN_AU"),
        LanguageMapping(culture_id="it-CH", lang_id=74, code="swiss-italian", short_code="IT_CH"),
        LanguageMapping(culture_id="en-PH", lang_id=75, code="phillippine-english", short_code="EN_PH"),
        LanguageMapping(culture_id="fl-PH", lang_id=76, code="filipino", short_code="FL_PH"),
        LanguageMapping(culture_id="en-CA", lang_id=77, code="canadian-english", short_code="EN_CA"),
    ]

    # Supported language IDs for this project (10 languages)
    # EN, NL, FR, DE, IT, ES, PT, ZH, HU, TH
    SUPPORTED_LANGUAGE_IDS: list[int] = [1, 2, 3, 4, 5, 6, 11, 12, 16, 51]

    # Build lookup dictionaries for faster access
    _by_culture_id: dict[str, LanguageMapping] = {}
    _by_lang_id: dict[int, LanguageMapping] = {}
    _by_short_code: dict[str, LanguageMapping] = {}
    _by_code: dict[str, LanguageMapping] = {}

    @classmethod
    def _ensure_lookups(cls) -> None:
        """Build lookup dictionaries if not already built."""
        if not cls._by_culture_id:
            for mapping in cls.ICECAT_LANGUAGE_MAPPING:
                cls._by_culture_id[mapping.culture_id.lower()] = mapping
                cls._by_lang_id[mapping.lang_id] = mapping
                cls._by_short_code[mapping.short_code.lower()] = mapping
                cls._by_code[mapping.code.lower()] = mapping

    @classmethod
    def map_to_culture_id(
        cls,
        lang_id: int | str | None = None,
        code: str | None = None,
        short_code: str | None = None,
    ) -> str | None:
        """
        Map Icecat language properties to culture ID.

        Args:
            lang_id: Icecat language ID
            code: Icecat language code (e.g., "english")
            short_code: Icecat short code (e.g., "EN")

        Returns:
            Culture ID or None if not found
        """
        cls._ensure_lookups()

        if lang_id is not None:
            lid = int(lang_id) if isinstance(lang_id, str) else lang_id
            if lid in cls._by_lang_id:
                return cls._by_lang_id[lid].culture_id

        if code is not None:
            if code.lower() in cls._by_code:
                return cls._by_code[code.lower()].culture_id

        if short_code is not None:
            if short_code.lower() in cls._by_short_code:
                return cls._by_short_code[short_code.lower()].culture_id

        return None

    @classmethod
    def map_to_icecat_short_code(cls, culture_id: str) -> str | None:
        """
        Map local culture ID to Icecat short code.

        Args:
            culture_id: Local culture ID (e.g., "en-US")

        Returns:
            Icecat short code (e.g., "US") or None if not found
        """
        cls._ensure_lookups()
        mapping = cls._by_culture_id.get(culture_id.lower())
        return mapping.short_code if mapping else None

    @classmethod
    def map_to_icecat_lang_id(
        cls,
        culture_id: str | None = None,
        short_code: str | None = None,
    ) -> int | None:
        """
        Map local culture ID or short code to Icecat language ID.

        Args:
            culture_id: Local culture ID (e.g., "en-US")
            short_code: Icecat short code (e.g., "US")

        Returns:
            Icecat language ID or None if not found
        """
        cls._ensure_lookups()

        if culture_id is not None:
            mapping = cls._by_culture_id.get(culture_id.lower())
            if mapping:
                return mapping.lang_id

        if short_code is not None:
            mapping = cls._by_short_code.get(short_code.lower())
            if mapping:
                return mapping.lang_id

        return None

    @classmethod
    def map_to_icecat_lang_id_str(
        cls,
        culture_id: str | None = None,
        short_code: str | None = None,
    ) -> str | None:
        """
        Map local culture ID or short code to Icecat language ID as string.

        Args:
            culture_id: Local culture ID (e.g., "en-US")
            short_code: Icecat short code (e.g., "US")

        Returns:
            Icecat language ID as string or None if not found
        """
        lang_id = cls.map_to_icecat_lang_id(culture_id, short_code)
        return str(lang_id) if lang_id is not None else None

    @classmethod
    def get_all_language_mappings(cls) -> list[LanguageMapping]:
        """Get all language mappings."""
        return list(cls.ICECAT_LANGUAGE_MAPPING)

    @classmethod
    def get_mapping_by_culture_id(cls, culture_id: str) -> LanguageMapping | None:
        """Get full mapping by culture ID."""
        cls._ensure_lookups()
        return cls._by_culture_id.get(culture_id.lower())

    @classmethod
    def get_mapping_by_lang_id(cls, lang_id: int) -> LanguageMapping | None:
        """Get full mapping by Icecat language ID."""
        cls._ensure_lookups()
        return cls._by_lang_id.get(lang_id)

    @classmethod
    def get_supported_languages(cls) -> list[LanguageMapping]:
        """
        Get only the supported language mappings for this project.

        Returns:
            List of 10 supported LanguageMapping objects (EN, NL, FR, DE, IT, ES, PT, ZH, HU, TH)
        """
        cls._ensure_lookups()
        return [
            cls._by_lang_id[lang_id]
            for lang_id in cls.SUPPORTED_LANGUAGE_IDS
            if lang_id in cls._by_lang_id
        ]

    @classmethod
    def is_supported_language(cls, lang_id: int) -> bool:
        """
        Check if a language ID is in the supported list.

        Args:
            lang_id: Icecat language ID to check

        Returns:
            True if the language is supported, False otherwise
        """
        return lang_id in cls.SUPPORTED_LANGUAGE_IDS

    @classmethod
    def get_short_code_by_lang_id(cls, lang_id: int) -> str | None:
        """
        Get short code (e.g., 'EN') from language ID.

        Args:
            lang_id: Icecat language ID

        Returns:
            Short code string or None if not found
        """
        mapping = cls.get_mapping_by_lang_id(lang_id)
        return mapping.short_code if mapping else None
