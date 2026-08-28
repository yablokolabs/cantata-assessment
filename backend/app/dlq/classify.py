"""Classification — map a step failure onto a FailureClass.

Classification happens at capture time, from the exception type, because that
is the only moment the failure's cause is still known. Deciding at replay time
means guessing from a traceback string. See DESIGN.md section 3.

The taxonomy below is not invented: the steps already define distinct exception
types for exactly these cases, and nothing was ever wired up to read them.
"""

from __future__ import annotations

import httpx
from pydantic import ValidationError

from app.models import FailureClass
from app.pipeline.steps.auto_qa_invite import SmtpTransientError
from app.pipeline.steps.delivery import CustomerWebhookError
from app.pipeline.steps.manual_qa_submit import QaSubmissionTimeoutError
from app.pipeline.steps.stt_submit import SttVendorError

# A step that returns success=False rather than raising. Both such sites in the
# codebase are missing-precondition errors ("no callback payload", "no reviewed
# transcript in stores") -- a replay finds the same data absent and fails again.
SOFT_FAILURE_CLASS = FailureClass.POISON

_RULES: tuple[tuple[type[BaseException], FailureClass], ...] = (
    # Malformed vendor callback. Replay re-parses the same bytes and fails identically.
    (ValidationError, FailureClass.POISON),
    # The editor missed their SLA. No machine retry can satisfy this.
    (QaSubmissionTimeoutError, FailureClass.NEEDS_HUMAN),
    # Dependency was down; the side effect did not land.
    (SmtpTransientError, FailureClass.TRANSIENT),
    (CustomerWebhookError, FailureClass.TRANSIENT),
    # The vendor accepted the job but we crashed before committing the job id.
    # Neither TRANSIENT (the side effect DID land) nor POISON (a replay would
    # "succeed", by double-submitting and double-billing). UNKNOWN forces an
    # operator decision. DESIGN.md section 3 argues for a fifth class here.
    (SttVendorError, FailureClass.UNKNOWN),
)


def classify(exc: BaseException) -> FailureClass:
    """Return the failure class for a raised step exception."""
    if isinstance(exc, httpx.HTTPStatusError):
        # 4xx is a request we will keep getting wrong no matter how often we
        # send it; only 5xx is worth replaying.
        if exc.response.status_code >= 500:
            return FailureClass.TRANSIENT
        return FailureClass.POISON

    for exc_type, failure_class in _RULES:
        if isinstance(exc, exc_type):
            return failure_class

    # Conservative default: UNKNOWN never auto-retries and never replays without
    # an explicit operator override.
    return FailureClass.UNKNOWN
