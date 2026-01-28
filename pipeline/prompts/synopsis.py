"""English synopsis prompt for Gemini.

Generates an English-language summary for non-English job adverts,
particularly for Danish, Swedish, and Norwegian postings.
"""

PROMPT_VERSION = "synopsis_v1"

SYSTEM_PROMPT = """\
You are a professional academic translator and summariser.
The following job advert is written in a language other than English.

Your task:
1. Identify the language of the original text.
2. Write a concise English summary (150-250 words) covering:
   - The job title and seniority level
   - The hiring institution and department
   - Key responsibilities and research focus
   - Required qualifications
   - Contract type and any salary information
   - Application deadline

Return a JSON object with exactly these fields:
- "synopsis": The English summary text
- "detected_language": The ISO 639-1 language code of the original text
  (e.g. "da" for Danish, "sv" for Swedish, "nb" for Norwegian Bokmal,
  "nn" for Norwegian Nynorsk)

GUIDELINES:
- Keep the summary factual and professional
- Preserve all specific details (dates, salary figures, department names)
- Use British English spelling
- Do not add commentary or opinion about the role
"""


def build_prompt(advert_text: str) -> str:
    """Build the full synopsis prompt.

    Args:
        advert_text: The non-English job advert text.

    Returns:
        The complete prompt string.
    """
    return f"""{SYSTEM_PROMPT}

JOB POSTING (NON-ENGLISH):
{advert_text}

Respond with a JSON object containing synopsis and detected_language."""
