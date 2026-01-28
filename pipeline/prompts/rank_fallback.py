"""Rank fallback classification prompt for Gemini.

Used when regex-based rank mapping fails to classify a job title.
Gemini determines the appropriate rank bucket.
"""

PROMPT_VERSION = "rank_fallback_v1"

SYSTEM_PROMPT = """\
You are an expert in international academic career structures.
Classify the following job title into one of these rank buckets:

RANK BUCKETS:
- "professor": Full Professor, Chair, Distinguished Professor
- "associate_professor": Associate Professor, Senior Lecturer, Reader,
  Principal Lecturer, Docent, Lektor, Universitetslektor
- "assistant_professor": Assistant Professor, Lecturer, Adjunkt, Junior Lecturer
- "research_fellow": Research Fellow, Senior Research Fellow, Forsker, Seniorforsker
- "postdoc": Postdoctoral Researcher, Postdoc, Research Associate (postdoctoral level)
- "other": Administrative roles, PhD positions, technical roles, or anything
  that does not fit the above categories

CONTEXT:
- Consider international equivalences (UK, US, Nordic, EU, Australian systems)
- The target audience is a mid-career academic at Senior Lecturer / Associate
  Professor level, so accuracy at that boundary is crucial

Return a JSON object with exactly these fields:
- "rank_bucket": one of the six bucket names above
- "confidence": a float between 0.0 and 1.0
- "reasoning": a brief explanation of your classification
"""


def build_prompt(job_title: str) -> str:
    """Build the full rank fallback prompt.

    Args:
        job_title: The job title to classify.

    Returns:
        The complete prompt string.
    """
    return f"""{SYSTEM_PROMPT}

JOB TITLE:
{job_title}

Respond with a JSON object containing rank_bucket, confidence, and reasoning."""
