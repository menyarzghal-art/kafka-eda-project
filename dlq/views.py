from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .agent import verified_suggestion
from .models import DeadLetter
from .utils import ReplayRefused, get_producer, replay


def dlq_list(request):
    return render(request, "dlq/list.html", {"letters": DeadLetter.objects.order_by("-id")})


def _page(request, dl, flash=None, ok=True):
    return render(request, "dlq/detail.html",
                  {"dl": dl, "editor": dl.suggestion or dl.payload, "flash": flash, "ok": ok})


def dlq_detail(request, pk):
    return _page(request, get_object_or_404(DeadLetter, pk=pk))


def _suggest(dl):
    ok, res = verified_suggestion(dl.payload, dl.error, dl.stacktrace)
    if ok:
        dl.suggestion = res
        dl.save(update_fields=["suggestion"])
    return ok, res


@require_POST
def dlq_suggest(request, pk):
    dl = get_object_or_404(DeadLetter, pk=pk)
    ok, res = _suggest(dl)
    return _page(request, dl, "Suggestion validée par Pydantic" if ok else f"Suggestion rejetée : {res}", ok)


@require_POST
def dlq_replay(request, pk):
    dl = get_object_or_404(DeadLetter, pk=pk)
    try:
        replay(dl, request.POST.get("payload", ""), get_producer())
    except ReplayRefused as exc:
        return _page(request, dl, f"Rejeu refusé : {exc}", False)
    return _page(request, dl, "Message rejoué dans le topic orders", True)


@csrf_exempt
@require_POST
def api_suggest(request, pk):
    """Appelé par n8n."""
    ok, res = _suggest(get_object_or_404(DeadLetter, pk=pk))
    return JsonResponse({"dlq_id": pk, "valid": ok, "suggestion" if ok else "errors": res})
