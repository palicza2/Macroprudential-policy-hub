from __future__ import annotations

import html
import re
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import requests


def parse_news_date(text: str) -> str:
    if not text:
        return ""
    text = str(text)
    iso_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if iso_match:
        return iso_match.group(1)

    month_map = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7,
        "july": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
    }

    match = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", text)
    if match:
        month = month_map.get(match.group(1).lower())
        if month:
            try:
                return datetime(int(match.group(3)), month, int(match.group(2))).strftime("%Y-%m-%d")
            except Exception:
                return ""

    match = re.search(r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})", text)
    if match:
        month = month_map.get(match.group(2).lower())
        if month:
            try:
                return datetime(int(match.group(3)), month, int(match.group(1))).strftime("%Y-%m-%d")
            except Exception:
                return ""

    return ""


def fetch_news(*, api_key: str, cse_id: str, query: str, months_back: int, max_results: int) -> pd.DataFrame:
    if not api_key or not cse_id:
        return pd.DataFrame()

    params = {"key": api_key, "cx": cse_id, "q": query, "dateRestrict": f"m{months_back}", "num": max_results}
    resp = requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=20)
    resp.raise_for_status()
    items = resp.json().get("items", [])[:max_results]

    date_keys = [
        "article:published_time", "og:published_time", "date", "dc.date",
        "dc.date.issued", "citation_publication_date", "citation_date", "pubdate",
    ]

    rows = []
    for item in items:
        link = item.get("link", "")
        title = item.get("title", "")
        snippet = item.get("snippet", "")
        source = ""
        if link:
            try:
                source = link.split("/")[2]
            except Exception:
                source = ""

        raw_date = ""
        for meta in item.get("pagemap", {}).get("metatags", []):
            if not isinstance(meta, dict):
                continue
            for key in date_keys:
                if key in meta:
                    raw_date = meta.get(key, "")
                    break
            if raw_date:
                break

        parsed_date = parse_news_date(raw_date)
        date_source = "meta" if parsed_date else ""
        if not parsed_date:
            parsed_date = parse_news_date(snippet)
            date_source = "snippet" if parsed_date else ""
        if not parsed_date:
            parsed_date = parse_news_date(title)
            date_source = "title" if parsed_date else ""

        rows.append({"TITLE": title, "SOURCE": source, "SUMMARY": snippet, "LINK": link, "DATE": parsed_date, "DATE_SOURCE": date_source})

    df = pd.DataFrame(rows)
    if not df.empty:
        df["DATE_PARSED"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.sort_values("DATE_PARSED", ascending=False, na_position="last")
    return df


def slug_to_label(slug: str) -> str:
    labels = {
        "ccyb": "CCyB",
        "syrb": "SyRB",
        "bbm": "BBM",
        "ltv": "LTV",
        "dsti": "DSTI",
        "lti": "LTI",
        "dti": "DTI",
        "real-estate": "Real Estate",
        "capital": "Capital",
        "reciprocation": "Reciprocation",
    }
    return labels.get(slug, slug.title())


def extract_news_tags(text: str) -> List[Tuple[str, str]]:
    text = (text or "").lower()
    tag_defs = [
        ("CCyB", "ccyb", ["ccyb", "countercyclical capital buffer", "countercyclical buffer"]),
        ("SyRB", "syrb", ["syrb", "systemic risk buffer"]),
        ("BBM", "bbm", ["borrower-based", "bbm", "borrower based"]),
        ("LTV", "ltv", ["ltv", "loan-to-value"]),
        ("DSTI", "dsti", ["dsti", "debt-service-to-income"]),
        ("LTI", "lti", ["lti", "loan-to-income"]),
        ("DTI", "dti", ["dti", "debt-to-income"]),
        ("Real Estate", "real-estate", ["real estate", "property", "housing", "mortgage"]),
        ("Capital", "capital", ["capital buffer", "capital requirement", "capital"]),
        ("Reciprocation", "reciprocation", ["reciprocation", "reciprocity"]),
    ]
    tags: List[Tuple[str, str]] = []
    for label, slug, terms in tag_defs:
        if any(term in text for term in terms):
            tags.append((label, slug))
        if len(tags) >= 4:
            break
    return tags


def build_source_initials(source: str) -> str:
    if not source:
        return "N"
    parts = re.split(r"[\.\-]", source)
    letters = [p[0].upper() for p in parts if p]
    return "".join(letters[:2]) if letters else source[:2].upper()


def detect_countries(text: str) -> List[str]:
    text = (text or "").lower()
    countries = [
        "austria", "belgium", "bulgaria", "croatia", "cyprus", "czech republic",
        "denmark", "estonia", "finland", "france", "germany", "greece",
        "hungary", "ireland", "italy", "latvia", "lithuania", "luxembourg",
        "malta", "netherlands", "poland", "portugal", "romania", "slovakia",
        "slovenia", "spain", "sweden", "norway", "switzerland", "iceland",
        "united kingdom", "uk", "england",
    ]
    found: List[str] = []
    for country in countries:
        if re.search(rf"\\b{re.escape(country)}\\b", text):
            label = "United Kingdom" if country in ("uk", "england") else country.title()
            if label not in found:
                found.append(label)
        if len(found) >= 3:
            break
    return found


def build_news_feed_html(df: pd.DataFrame, *, today_str: str) -> str:
    if df is None or df.empty:
        return "<div class='empty-state'>No news available.</div>"

    cards: List[str] = []
    for _, row in df.iterrows():
        title = html.escape(str(row.get("TITLE", "")).strip())
        summary_raw = str(row.get("SUMMARY_SHORT", "")).strip() or str(row.get("SUMMARY", "")).strip()
        summary = html.escape(summary_raw)
        link = html.escape(str(row.get("LINK", "")).strip())
        source_raw = str(row.get("SOURCE", "")).strip()
        source = html.escape(source_raw)
        date_text = html.escape(str(row.get("DATE", "")).strip())
        date_source = str(row.get("DATE_SOURCE", "")).strip()

        llm_tags = row.get("TAGS") if isinstance(row.get("TAGS"), list) else []
        tags = [(slug_to_label(slug), slug) for slug in llm_tags] or extract_news_tags(f"{title} {summary}")
        tag_html = "".join([f"<span class='news-tag news-tag--{slug}'>{label}</span>" for label, slug in tags])
        tag_slugs = " ".join([slug for _, slug in tags])

        search_text = html.escape(f"{title} {summary} {source}").lower()
        icon_text = build_source_initials(source)
        favicon = f"https://www.google.com/s2/favicons?domain={source_raw}&sz=64" if source_raw else ""

        country_list = detect_countries(f"{title} {summary}")
        countries_html = "".join([f"<span class='news-pill'>{html.escape(c)}</span>" for c in country_list])

        published_label = "Published" if date_source == "meta" else "Reported"
        date_line = f"<span class='news-date'>{published_label}: {date_text}</span>" if date_text else ""

        cards.append(
            f"""
            <article class="news-card" data-tags="{tag_slugs}" data-search="{search_text}">
                <div class="news-card__header">
                    <div class="news-tags">{tag_html}</div>
                    <div class="news-meta">
                        {date_line}
                        <span class="news-date">Retrieved: {today_str}</span>
                    </div>
                </div>
                <h3 class="news-title">{title or "Untitled update"}</h3>
                {f"<div class='news-countries'>{countries_html}</div>" if countries_html else ""}
                <p class="news-summary">{summary or "No summary available."}</p>
                <div class="news-divider"></div>
                <div class="news-actions">
                    <div class="news-source">
                        <span class="news-source__icon">
                            {f'<img class="news-source__favicon" src="{favicon}" alt="">' if favicon else icon_text}
                        </span>
                        <span>{source or "Source"}</span>
                    </div>
                    <a class="news-link" href="{link}" target="_blank" rel="noopener">
                        <span class="news-link__icon" aria-hidden="true"><i data-lucide="share-2"></i></span>
                        Read original
                    </a>
                </div>
            </article>
            """
        )

    return f"<div class='news-feed'>{''.join(cards)}</div>"

