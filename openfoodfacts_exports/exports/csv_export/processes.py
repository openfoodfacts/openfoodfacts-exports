from datetime import datetime


def process_url(lang: str, code: str) -> str:
    base_url = f"http://{lang}.openfoodfacts.org/product/"
    return base_url + code


def process_tags(tags: list[str]) -> str:
    return ",".join(tags)


def process_lang(tags: list[str], lang: str) -> str:
    """We extract from 'tags' all element tagged by the appropriate language

    Example:
    - [en:plastic, en:tray, fr:barquette-en-plastique, fr:film-en-plastique]
    - [en:glass, en:bottle]

    -Output: glass if lang="en"
    """
    return ",".join([tag.split(":")[-1] for tag in tags if tag.startswith(f"{lang}:")])


def process_text(texts: list[dict], lang: str) -> str:
    """In the Parquet, texts are grouped by lang as a dict as such: 
    {
        "lang": "main", "text": <text>,
        "lang": "pl", "text": <text>
    }"""
    for elt in texts:
        return elt["text"] if elt["lang"] == lang else elt.get("main", "")
