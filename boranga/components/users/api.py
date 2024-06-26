import logging

from django.core.cache import cache
from django.db import transaction
from django.db.models import CharField, Value
from django.db.models.functions import Concat
from django.shortcuts import get_object_or_404
from django_countries import countries
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from rest_framework import mixins, views, viewsets
from rest_framework.decorators import action as detail_route
from rest_framework.decorators import action as list_route
from rest_framework.decorators import renderer_classes
from rest_framework.exceptions import PermissionDenied
from rest_framework.permissions import IsAuthenticated
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response

from boranga.components.conservation_status.models import ConservationStatusReferral
from boranga.components.main.utils import retrieve_department_users
from boranga.components.occurrence.models import OccurrenceReportReferral
from boranga.components.users.models import SubmitterCategory, SubmitterInformation
from boranga.components.users.serializers import (
    EmailUserActionSerializer,
    EmailUserCommsSerializer,
    EmailUserLogEntrySerializer,
    SubmitterCategorySerializer,
    SubmitterInformationSerializer,
    UserSerializer,
)
from boranga.permissions import IsApprover, IsAssessor, IsInternal

logger = logging.getLogger(__name__)


class DepartmentUserList(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsInternal]

    def get(self, request, format=None):
        data = cache.get("department_users")
        if not data:
            retrieve_department_users()
            data = cache.get("department_users")
        data = retrieve_department_users()
        return Response(data)


class GetCountries(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsAuthenticated]

    def get(self, request, format=None):
        country_list = []
        for country in list(countries):
            country_list.append({"name": country.name, "code": country.code})
        return Response(country_list)


class GetProfile(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsAuthenticated]

    def get(self, request, format=None):
        serializer = UserSerializer(request.user, context={"request": request})
        return Response(serializer.data)


class GetSubmitterCategories(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsAuthenticated]

    def get(self, request, format=None):
        submitter_categories = SubmitterCategory.objects.all()
        serializer = SubmitterCategorySerializer(submitter_categories, many=True)
        return Response(serializer.data)


class SaveSubmitterInformation(views.APIView):
    renderer_classes = [
        JSONRenderer,
    ]
    permission_classes = [IsAuthenticated]

    def put(self, request, format=None):
        instance = get_object_or_404(SubmitterInformation, pk=request.data["id"])
        if not instance.email_user == request.user.id:
            raise PermissionDenied("You do not have permission to perform this action.")

        serializer = SubmitterInformationSerializer(
            instance=instance, data=request.data, context={"request": request}
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data)


class UserViewSet(viewsets.GenericViewSet, mixins.RetrieveModelMixin):
    queryset = EmailUser.objects.all()
    serializer_class = UserSerializer
    permission_classes = [IsAssessor | IsApprover]

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_users(self, request, *args, **kwargs):
        search_term = request.GET.get("term", "")

        # Allow for search of first name, last name and concatenation of both
        users = EmailUser.objects.annotate(
            search_term=Concat(
                "first_name",
                Value(" "),
                "last_name",
                Value(" "),
                "email",
                output_field=CharField(),
            )
        )
        if kwargs.get("is_staff", False):
            users = users.filter(is_staff=True)

        id_field = "email"
        if kwargs.get("id_field", False):
            id_field = kwargs.get("id_field")

        users = users.filter(search_term__icontains=search_term).values(
            "id", "email", "first_name", "last_name"
        )[:10]

        data_transform = [
            {
                "id": person[id_field],
                "text": f"{person['first_name']} {person['last_name']} ({person['email']})",
            }
            for person in users
        ]
        return Response({"results": data_transform})

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_users_ledger_id(self, request, *args, **kwargs):
        return self.get_users(request, id_field="id")

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_department_users(self, request):
        return self.get_users(request, is_staff=True)

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_department_users_ledger_id(self, request, *args, **kwargs):
        return self.get_users(request, is_staff=True, id_field="id")

    @list_route(
        methods=[
            "GET",
        ],
        detail=False,
    )
    def get_referees(self, request, *args, **kwargs):
        search_term = request.GET.get("term", "")

        if not search_term:
            return Response({"results": []})

        # Allow for search of first name, last name and concatenation of both
        department_users = EmailUser.objects.annotate(
            search_term=Concat(
                "first_name",
                Value(" "),
                "last_name",
                Value(" "),
                "email",
                output_field=CharField(),
            )
        ).filter(is_staff=True)

        department_users = department_users.filter(
            search_term__icontains=search_term
        ).values("id", "email", "first_name", "last_name")[:10]
        external_cs_referrals = ConservationStatusReferral.objects.filter(
            is_external=True
        ).values_list("referral", flat=True)
        external_ocr_referrals = OccurrenceReportReferral.objects.filter(
            is_external=True
        ).values_list("referral", flat=True)
        external_referee_ids = list(
            set(list(external_cs_referrals) + list(external_ocr_referrals))
        )

        external_referees = EmailUser.objects.filter(
            id__in=external_referee_ids
        ).annotate(
            search_term=Concat(
                "first_name",
                Value(" "),
                "last_name",
                Value(" "),
                "email",
                output_field=CharField(),
            )
        )
        external_referees = external_referees.filter(
            search_term__icontains=search_term
        ).values("id", "email", "first_name", "last_name")[:10]

        internal = {
            "text": "Internal",
            "children": [
                {
                    "id": person["email"],
                    "text": f"{person['first_name']} {person['last_name']} ({person['email']})",
                }
                for person in department_users
            ],
        }
        external = {
            "text": "External ",
            "children": [
                {
                    "id": person["email"],
                    "text": f"{person['first_name']} {person['last_name']} ({person['email']})",
                }
                for person in external_referees
            ],
        }

        data_transform = []
        if department_users.exists():
            data_transform.append(internal)
        if external_referees.exists():
            data_transform.append(external)

        return Response({"results": data_transform})

    @detail_route(
        methods=[
            "GET",
        ],
        detail=True,
    )
    def action_log(self, request, *args, **kwargs):
        instance = self.get_object()
        qs = instance.action_logs.all()
        serializer = EmailUserActionSerializer(qs, many=True)
        return Response(serializer.data)

    @detail_route(
        methods=[
            "GET",
        ],
        detail=True,
    )
    def comms_log(self, request, *args, **kwargs):
        instance = self.get_object()
        qs = instance.comms_logs.all()
        serializer = EmailUserCommsSerializer(qs, many=True)
        return Response(serializer.data)

    @detail_route(
        methods=[
            "POST",
        ],
        detail=True,
    )
    @renderer_classes((JSONRenderer,))
    @transaction.atomic
    def add_comms_log(self, request, *args, **kwargs):
        instance = self.get_object()
        mutable = request.data._mutable
        request.data._mutable = True
        request.data["emailuser"] = f"{instance.id}"
        request.data["staff"] = f"{request.user.id}"
        request.data._mutable = mutable
        serializer = EmailUserLogEntrySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        comms = serializer.save()

        # Save the files
        for f in request.FILES:
            document = comms.documents.create()
            document.name = str(request.FILES[f])
            document._file = request.FILES[f]
            document.save()

        return Response(serializer.data)
