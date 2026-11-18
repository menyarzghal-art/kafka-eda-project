from django.urls import path
from . import views

urlpatterns = [
    path("", views.dlq_list, name="dlq_list"),
    path("<int:pk>/", views.dlq_detail, name="dlq_detail"),
    path("<int:pk>/suggest/", views.dlq_suggest, name="dlq_suggest"),
    path("<int:pk>/replay/", views.dlq_replay, name="dlq_replay"),
    path("api/<int:pk>/suggest/", views.api_suggest, name="dlq_api_suggest"),
]
