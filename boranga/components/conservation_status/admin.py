from django.contrib.gis import admin

from boranga.admin import ArchivableModelAdminMixin, DeleteProtectedModelAdmin
from boranga.components.conservation_status import models


@admin.register(models.ProposalAmendmentReason)
class ProposalAmendmentReasonAdmin(
    ArchivableModelAdminMixin, DeleteProtectedModelAdmin
):
    list_display = ["reason"]


class AbstractListAdmin(DeleteProtectedModelAdmin):
    list_display = [
        "code",
        "label",
        "applies_to_flora",
        "applies_to_fauna",
        "applies_to_communities",
    ]


class AbstractCategoryAdmin(DeleteProtectedModelAdmin):
    list_display = ["code", "label"]


class WAPriorityListAdmin(ArchivableModelAdminMixin, AbstractListAdmin):
    pass


class WAPriorityCategoryAdmin(ArchivableModelAdminMixin, AbstractCategoryAdmin):
    filter_horizontal = ("wa_priority_lists",)


class IUCNVersionAdmin(ArchivableModelAdminMixin, AbstractListAdmin):
    pass


class WALegislativeListAdmin(ArchivableModelAdminMixin, AbstractListAdmin):
    pass


class WALegislativeCategoryAdmin(ArchivableModelAdminMixin, AbstractCategoryAdmin):
    filter_horizontal = ("wa_legislative_lists",)


class CommonwealthConservationListAdmin(ArchivableModelAdminMixin, AbstractListAdmin):
    pass


class OtherConservationAssessmentListAdmin(
    ArchivableModelAdminMixin, AbstractListAdmin
):
    pass


class ConservationChangeCodeAdmin(ArchivableModelAdminMixin, DeleteProtectedModelAdmin):
    list_display = ["code", "label"]


admin.site.register(models.WAPriorityList, WAPriorityListAdmin)
admin.site.register(models.WAPriorityCategory, WAPriorityCategoryAdmin)
admin.site.register(models.WALegislativeList, WALegislativeListAdmin)
admin.site.register(models.WALegislativeCategory, WALegislativeCategoryAdmin)
admin.site.register(models.IUCNVersion, IUCNVersionAdmin)
admin.site.register(
    models.CommonwealthConservationList, CommonwealthConservationListAdmin
)
admin.site.register(
    models.OtherConservationAssessmentList, OtherConservationAssessmentListAdmin
)
admin.site.register(models.ConservationChangeCode, ConservationChangeCodeAdmin)
