import logging

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from django.http import HttpRequest
from django.utils import timezone

from boranga.components.species_and_communities.email import (
    send_community_create_email_notification,
    send_species_create_email_notification,
    send_species_rename_email_notification,
    send_user_community_create_email_notification,
    send_user_species_create_email_notification,
)
from boranga.components.species_and_communities.models import (
    Community,
    CommunityUserAction,
    ConservationThreat,
    Species,
    SpeciesConservationAttributes,
    SpeciesDistribution,
    SpeciesDocument,
    SpeciesUserAction,
)

logger = logging.getLogger(__name__)


@transaction.atomic
def species_form_submit(species_instance, request, split=False):
    if split:
        if not species_instance.can_user_split:
            raise ValidationError("You can't split this species at this moment")
    else:
        if not species_instance.can_user_edit:
            raise ValidationError("You can't submit this species at this moment")

    species_instance.submitter = request.user.id
    species_instance.lodgement_date = timezone.now()

    # Create a log entry for the proposal
    species_instance.log_user_action(
        SpeciesUserAction.ACTION_CREATE_SPECIES.format(species_instance.species_number),
        request,
    )

    # Create a log entry for the user
    request.user.log_user_action(
        SpeciesUserAction.ACTION_CREATE_SPECIES.format(species_instance.species_number),
        request,
    )

    ret1 = send_species_create_email_notification(request, species_instance)
    ret2 = send_user_species_create_email_notification(request, species_instance)

    if (settings.WORKING_FROM_HOME and settings.DEBUG) or ret1 and ret2:
        species_instance.processing_status = Species.PROCESSING_STATUS_ACTIVE
        # all functions that call this save after - otherwise we can parametise this if need be
        species_instance.save(no_revision=True)
    else:
        raise ValidationError(
            "An error occurred while submitting proposal (Submit email notifications failed)"
        )
    return species_instance


@transaction.atomic
def community_form_submit(community_instance, request):
    if not community_instance.can_user_edit:
        raise ValidationError("You can't submit this community at this moment")

    community_instance.submitter = request.user.id
    community_instance.lodgement_date = timezone.now()

    # Create a log entry for the proposal
    community_instance.log_user_action(
        CommunityUserAction.ACTION_CREATE_COMMUNITY.format(
            community_instance.community_number
        ),
        request,
    )

    # Create a log entry for the user
    request.user.log_user_action(
        CommunityUserAction.ACTION_CREATE_COMMUNITY.format(
            community_instance.community_number
        ),
        request,
    )

    ret1 = send_community_create_email_notification(request, community_instance)
    ret2 = send_user_community_create_email_notification(request, community_instance)

    if (settings.WORKING_FROM_HOME and settings.DEBUG) or ret1 and ret2:
        community_instance.processing_status = Community.PROCESSING_STATUS_ACTIVE
        community_instance.save(no_revision=True)
    else:
        raise ValidationError(
            "An error occurred while submitting proposal (Submit email notifications failed)"
        )

    return community_instance


@transaction.atomic
def combine_species_original_submit(species_instance, request):
    if species_instance.processing_status != Species.PROCESSING_STATUS_ACTIVE:
        raise ValidationError("You can't submit this species at this moment")

    species_instance.processing_status = Species.PROCESSING_STATUS_HISTORICAL
    species_instance.save(version_user=request.user)

    return species_instance


@transaction.atomic
def rename_species_original_submit(species_instance, new_species, request):
    if species_instance.processing_status != Species.PROCESSING_STATUS_ACTIVE:
        raise ValidationError("You can't submit this species at this moment")

    species_instance.processing_status = Species.PROCESSING_STATUS_HISTORICAL
    species_instance.save(version_user=request.user)

    # send the rename species email notification
    send_species_rename_email_notification(request, species_instance, new_species)

    return species_instance


@transaction.atomic
def rename_deep_copy(instance: Species, request: HttpRequest) -> Species:
    # related items to instance that needs to create for new rename instance as well
    instance_documents = SpeciesDocument.objects.filter(species=instance.id)
    instance_threats = ConservationThreat.objects.filter(species=instance.id)
    instance_conservation_attributes = SpeciesConservationAttributes.objects.filter(
        species=instance.id
    )
    instance_distribution = SpeciesDistribution.objects.filter(species=instance.id)

    # clone the species instance into new rename instance
    new_rename_instance = Species.objects.get(pk=instance.pk)
    new_rename_instance.id = None
    new_rename_instance.taxonomy_id = None
    new_rename_instance.species_number = ""
    new_rename_instance.processing_status = Species.PROCESSING_STATUS_DRAFT
    new_rename_instance.lodgement_date = None
    new_rename_instance.save(version_user=request.user)

    # Copy the regions an districts
    new_rename_instance.regions.add(*instance.regions.all())
    new_rename_instance.districts.add(*instance.districts.all())

    # Log action
    new_rename_instance.log_user_action(
        SpeciesUserAction.ACTION_COPY_SPECIES_FROM.format(
            new_rename_instance.species_number,
            instance.species_number,
        ),
        request,
    )
    request.user.log_user_action(
        SpeciesUserAction.ACTION_COPY_SPECIES_FROM.format(
            new_rename_instance.species_number,
            instance.species_number,
        ),
        request,
    )
    instance.log_user_action(
        SpeciesUserAction.ACTION_COPY_SPECIES_TO.format(
            instance.species_number,
            new_rename_instance.species_number,
        ),
        request,
    )
    request.user.log_user_action(
        SpeciesUserAction.ACTION_COPY_SPECIES_TO.format(
            instance.species_number,
            new_rename_instance.species_number,
        ),
        request,
    )

    for new_document in instance_documents:
        new_doc_instance = new_document
        new_doc_instance.species = new_rename_instance
        new_doc_instance.id = None
        new_doc_instance.document_number = ""
        new_doc_instance.save(version_user=request.user)
        new_doc_instance.species.log_user_action(
            SpeciesUserAction.ACTION_ADD_DOCUMENT.format(
                new_doc_instance.document_number,
                new_doc_instance.species.species_number,
            ),
            request,
        )
        request.user.log_user_action(
            SpeciesUserAction.ACTION_ADD_DOCUMENT.format(
                new_doc_instance.document_number,
                new_doc_instance.species.species_number,
            ),
            request,
        )

    for new_threat in instance_threats:
        new_threat_instance = new_threat
        new_threat_instance.species = new_rename_instance
        new_threat_instance.id = None
        new_threat_instance.threat_number = ""
        new_threat_instance.save(version_user=request.user)
        new_threat_instance.species.log_user_action(
            SpeciesUserAction.ACTION_ADD_THREAT.format(
                new_threat_instance.threat_number,
                new_threat_instance.species.species_number,
            ),
            request,
        )
        request.user.log_user_action(
            SpeciesUserAction.ACTION_ADD_THREAT.format(
                new_threat_instance.threat_number,
                new_threat_instance.species.species_number,
            ),
            request,
        )

    for new_cons_attr in instance_conservation_attributes:
        new_cons_attr_instance = new_cons_attr
        new_cons_attr_instance.species = new_rename_instance
        new_cons_attr_instance.id = None
        new_cons_attr_instance.save()

    for new_distribution in instance_distribution:
        new_distribution.species = new_rename_instance
        new_distribution.id = None
        new_distribution.save()

    return new_rename_instance
