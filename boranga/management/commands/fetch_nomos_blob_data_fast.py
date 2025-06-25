import logging

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from boranga.components.species_and_communities.email import send_nomos_script_failed
from boranga.components.species_and_communities.models import (
    ClassificationSystem,
    InformalGroup,
    Kingdom,
    Taxonomy,
    TaxonomyRank,
    TaxonPreviousName,
    TaxonVernacular,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Fetch Taxonomy data (Faster version)"

    def handle(self, *args, **options):
        """
        Fetches NOMOS blob data and updates the database using bulk operations.
        Ensures all referenced objects are saved before being used as FKs.
        """
        logger.info(f"Running command {__name__}")

        errors = []
        my_url = settings.NOMOS_BLOB_URL

        try:
            logger.info("Requesting NOMOS URL")
            headers = {"Accept-Encoding": "gzip, deflate"}
            taxon_res = requests.get(my_url, headers=headers, timeout=60)
            if taxon_res.status_code != 200:
                logger.error(f"Request failed with status code {taxon_res.status_code}")
                return

            logger.info("Done Fetching NOMOS data")
            taxon = taxon_res.json()

            # Prefetch all existing objects
            kingdom_map = {k.kingdom_id: k for k in Kingdom.objects.all()}
            rank_map = {r.taxon_rank_id: r for r in TaxonomyRank.objects.all()}
            taxonomy_map = {t.taxon_name_id: t for t in Taxonomy.objects.all()}
            vernacular_map = {v.vernacular_id: v for v in TaxonVernacular.objects.all()}
            class_system_map = {
                c.classification_system_id: c
                for c in ClassificationSystem.objects.all()
            }
            prev_name_map = {
                p.previous_name_id: p for p in TaxonPreviousName.objects.all()
            }

            new_kingdoms, new_ranks, new_taxonomies = [], [], []
            new_vernaculars, new_class_systems, new_informal_groups, new_prev_names = (
                [],
                [],
                [],
                [],
            )

            with transaction.atomic():
                # 1. Collect new Kingdoms
                for t in taxon:
                    kingdom_id = t.get("kingdom_id")
                    if kingdom_id and kingdom_id not in kingdom_map:
                        k = Kingdom(
                            kingdom_id=kingdom_id,
                            kingdom_name=t.get("kingdom_name", ""),
                        )
                        new_kingdoms.append(k)
                if new_kingdoms:
                    Kingdom.objects.bulk_create(
                        new_kingdoms, ignore_conflicts=True, batch_size=500
                    )
                kingdom_map = {k.kingdom_id: k for k in Kingdom.objects.all()}

                # 2. Collect new TaxonomyRanks
                for t in taxon:
                    rank_id = t.get("rank_id")
                    kingdom_id = t.get("kingdom_id")
                    if rank_id and rank_id not in rank_map:
                        r = TaxonomyRank(
                            taxon_rank_id=rank_id,
                            kingdom_id=kingdom_id,
                            kingdom_fk=kingdom_map.get(kingdom_id),
                            rank_name=t.get("rank_name", ""),
                        )
                        new_ranks.append(r)
                if new_ranks:
                    TaxonomyRank.objects.bulk_create(
                        new_ranks, ignore_conflicts=True, batch_size=500
                    )
                rank_map = {r.taxon_rank_id: r for r in TaxonomyRank.objects.all()}

                # 3. Collect new Taxonomies
                for t in taxon:
                    taxon_name_id = t.get("taxon_name_id")
                    if taxon_name_id and taxon_name_id not in taxonomy_map:
                        taxonomy = Taxonomy(
                            taxon_name_id=taxon_name_id,
                            scientific_name=t.get("canonical_name", ""),
                            kingdom_id=t.get("kingdom_id"),
                            kingdom_fk=kingdom_map.get(t.get("kingdom_id")),
                            kingdom_name=t.get("kingdom_name", ""),
                            name_authority=t.get("author", ""),
                            name_comments=t.get("notes", ""),
                            name_currency=t.get("is_current"),
                            taxon_rank_id=t.get("rank_id"),
                            taxonomy_rank_fk=rank_map.get(t.get("rank_id")),
                            family_id=t.get("family_id"),
                            family_name=t.get("family_canonical_name", ""),
                            genera_id=t.get("genus_id"),
                            genera_name=t.get("genus_canonical_name", ""),
                        )
                        new_taxonomies.append(taxonomy)
                if new_taxonomies:
                    Taxonomy.objects.bulk_create(
                        new_taxonomies, ignore_conflicts=True, batch_size=500
                    )
                taxonomy_map = {t.taxon_name_id: t for t in Taxonomy.objects.all()}

                # 4. Collect new ClassificationSystems
                for t in taxon:
                    for c in t.get("class_desc") or []:
                        class_system_id = c.get("id")
                        if class_system_id and class_system_id not in class_system_map:
                            new_class_systems.append(
                                ClassificationSystem(
                                    classification_system_id=class_system_id,
                                    class_desc=c.get("name", ""),
                                )
                            )
                if new_class_systems:
                    ClassificationSystem.objects.bulk_create(
                        new_class_systems, ignore_conflicts=True, batch_size=500
                    )
                class_system_map = {
                    c.classification_system_id: c
                    for c in ClassificationSystem.objects.all()
                }

                # 5. Collect new Vernaculars, InformalGroups, and PreviousNames
                for t in taxon:
                    taxon_obj = taxonomy_map.get(t.get("taxon_name_id"))
                    if not taxon_obj:
                        continue

                    # Vernaculars
                    for v in t.get("vernaculars") or []:
                        vernacular_id = v.get("id")
                        if vernacular_id and vernacular_id not in vernacular_map:
                            new_vernaculars.append(
                                TaxonVernacular(
                                    vernacular_id=vernacular_id,
                                    vernacular_name=v.get("name", ""),
                                    taxonomy=taxon_obj,
                                    taxon_name_id=taxon_obj.taxon_name_id,
                                )
                            )
                            vernacular_map[vernacular_id] = True

                    # InformalGroups
                    for c in t.get("class_desc") or []:
                        class_system_id = c.get("id")
                        if class_system_id:
                            new_informal_groups.append(
                                InformalGroup(
                                    taxonomy=taxon_obj,
                                    classification_system_fk=class_system_map.get(
                                        class_system_id
                                    ),
                                    classification_system_id=class_system_id,
                                    taxon_name_id=taxon_obj.taxon_name_id,
                                )
                            )

                    # PreviousNames
                    for p in t.get("previous_names") or []:
                        prev_name_id = p.get("id")
                        if prev_name_id and prev_name_id not in prev_name_map:
                            new_prev_names.append(
                                TaxonPreviousName(
                                    previous_name_id=prev_name_id,
                                    previous_scientific_name=p.get("name", ""),
                                    taxonomy=taxon_obj,
                                )
                            )
                            prev_name_map[prev_name_id] = True

                if new_vernaculars:
                    TaxonVernacular.objects.bulk_create(
                        new_vernaculars, ignore_conflicts=True, batch_size=500
                    )
                if new_informal_groups:
                    InformalGroup.objects.bulk_create(
                        new_informal_groups, ignore_conflicts=True, batch_size=500
                    )
                if new_prev_names:
                    TaxonPreviousName.objects.bulk_create(
                        new_prev_names, ignore_conflicts=True, batch_size=500
                    )

            logger.info(f"{len(taxon)} Taxon Records Updated. End")

        except Exception as e:
            logger.exception("Error at the end")
            errors.append(str(e))

        cmd_name = __name__.split(".")[-1].replace("_", " ").upper()
        err_str = (
            f'<strong style="color: red;">Errors: {len(errors)}</strong>'
            if errors
            else '<strong style="color: green;">Errors: 0</strong>'
        )
        msg = (
            f"{cmd_name} completed. Errors: {err_str}. Total IDs updated: {len(taxon)}."
        )
        logger.info(msg)

        if errors:
            send_nomos_script_failed(errors)
