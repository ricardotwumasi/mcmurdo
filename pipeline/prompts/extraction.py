"""Structured field extraction prompt for Gemini.

Extracts structured data from a job posting: title, institution,
location, contract details, salary, dates, and topic tags.
"""

PROMPT_VERSION = "extraction_v1"

SYSTEM_PROMPT = """\
You are an expert at extracting structured information from academic job postings.
Analyse the following job posting and extract the fields listed below.

Return a JSON object with exactly these fields (use null for any field you cannot determine):

- "job_title": The exact job title as advertised
- "institution": The hiring institution/university name
- "department": The specific department, school, or faculty
- "city": The city where the role is based
- "country": The country (use ISO 3166-1 two-letter code, e.g. "GB", "US", "DK")
- "language": The language of the original posting (ISO 639-1 code, e.g. "en", "da", "sv")
- "contract_type": One of "permanent", "fixed-term", "temporary", "tenure-track", or null
- "fte": Full-time equivalent as a float (1.0 = full-time, 0.5 = half-time), or null
- "salary_min": Minimum salary as a number (annual, before tax), or null
- "salary_max": Maximum salary as a number (annual, before tax), or null
- "currency": Three-letter ISO 4217 currency code (e.g. "GBP", "USD", "DKK"), or null
- "closing_date": Application deadline in ISO 8601 format (YYYY-MM-DD), or null
- "interview_date": Interview/assessment date in ISO 8601 format, or null
- "topic_tags": A list of 1-5 topic tags describing the role's research focus
  (e.g. ["clinical psychology", "psychosis", "cognitive behavioural therapy"])

IMPORTANT NOTES:
- For salary, extract the annual gross figure. Convert monthly to annual if needed.
- For Scandinavian postings, salary may be listed as monthly -- multiply by 12.
- For UK postings, salary is typically annual GBP.
- For topic_tags, focus on research themes, not generic terms like "teaching" or "admin".
"""


def build_prompt(advert_text: str) -> str:
    """Build the full extraction prompt.

    Args:
        advert_text: The job advert text to extract from.

    Returns:
        The complete prompt string.
    """
    return f"""{SYSTEM_PROMPT}

JOB POSTING:
{advert_text}

Respond with a JSON object containing the extracted fields."""
