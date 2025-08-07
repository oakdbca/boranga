<template lang="html">
    <div>
        <FormSection
            :form-collapse="false"
            label="Select the Resulting Species or Taxonomy"
            Index="species-combine-taxon-form-section-index"
        >
            <div class="row mb-3">
                <p>Select a species from this combine:</p>
                <ul class="list-group ps-3">
                    <li
                        v-for="(species, index) in existingSpeciesCombineList"
                        :key="species.id"
                        class="list-group-item"
                    >
                        <input
                            type="radio"
                            class="form-check-input me-2"
                            :id="
                                'resulting-species-from-combined-' + species.id
                            "
                            :value="index"
                            name="resulting-species-from-combined"
                            v-model="selectedSpeciesIndex"
                            @mousedown="
                                beforeChangeSelectedSpecies($event, index)
                            "
                        />
                        <span class="pe-2"
                            >{{ species.species_number }} -
                            {{ species.taxonomy_details.scientific_name }}
                        </span>
                        <span v-if="index == 0" class="badge bg-secondary"
                            >Original</span
                        >
                    </li>
                </ul>
            </div>
            <div class="row mb-3">
                <p class="mb-0 pb-0">or search for any other taxonomy:</p>
            </div>
            <div class="row mb-3">
                <div :id="select_scientific_name">
                    <select
                        :id="scientific_name_lookup"
                        :ref="scientific_name_lookup"
                        :name="scientific_name_lookup"
                        class="form-control"
                    />
                </div>
            </div>
        </FormSection>
        <div v-if="resultingSpecies" class="col-md-12">
            <ul id="pills-tab" class="nav nav-pills" role="tablist">
                <li class="nav-item">
                    <a
                        id="pills-profile-tab"
                        class="nav-link active"
                        data-bs-toggle="pill"
                        :href="'#' + profileTabId"
                        role="tab"
                        :aria-controls="profileTabId"
                        aria-selected="true"
                    >
                        Profile
                    </a>
                </li>
                <li class="nav-item">
                    <a
                        id="pills-combine-documents-tab"
                        class="nav-link"
                        data-bs-toggle="pill"
                        :href="'#' + documentTabId"
                        role="tab"
                        aria-controls="pills-combine-documents"
                        :aria-selected="documentTabId"
                    >
                        Documents
                    </a>
                </li>
                <li class="nav-item">
                    <a
                        id="pills-combine-threats-tab"
                        class="nav-link"
                        data-bs-toggle="pill"
                        :href="'#' + threatTabId"
                        role="tab"
                        :aria-controls="threatTabId"
                        aria-selected="false"
                    >
                        Threats
                    </a>
                </li>
            </ul>
            <div id="pills-tabContent" class="tab-content">
                <div
                    :id="profileTabId"
                    class="tab-pane fade show active"
                    role="tabpanel"
                    aria-labelledby="pills-profile-tab"
                >
                    <SpeciesProfile
                        v-if="resultingSpecies"
                        id="speciesInformation"
                        ref="species_information"
                        :species_community="resultingSpecies"
                        :species-being-combined="existingSpeciesCombineList"
                        :species_combine="true"
                    >
                    </SpeciesProfile>
                </div>
                <div
                    :id="documentTabId"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-combine-documents-tab"
                >
                    <div
                        v-for="(species, index) in existingSpeciesCombineList"
                        :key="index"
                    >
                        <SpeciesDocuments
                            :id="'species-combine-documents-' + index"
                            ref="species_combine_documents"
                            :species_community="resultingSpecies"
                            :species_original="species"
                        >
                        </SpeciesDocuments>
                    </div>
                </div>
                <div
                    :id="threatTabId"
                    class="tab-pane fade"
                    role="tabpanel"
                    aria-labelledby="pills-combine-threats-tab"
                >
                    <div
                        v-for="(species, index) in existingSpeciesCombineList"
                        :key="index"
                    >
                        <SpeciesThreats
                            :id="'species-combine-threats-' + index"
                            ref="species_combine_threats"
                            :species_community="resultingSpecies"
                            :species_original="species"
                        >
                        </SpeciesThreats>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';

import { api_endpoints } from '@/utils/hooks';

import FormSection from '@/components/forms/section_toggle.vue';

import SpeciesProfile from '@/components/common/species_communities/species_combine/species_combine_profile.vue';
import SpeciesDocuments from '@/components/common/species_communities/species_combine/species_combine_documents.vue';
import SpeciesThreats from '@/components/common/species_communities/species_combine/species_combine_threats.vue';

export default {
    components: {
        FormSection,
        SpeciesProfile,
        SpeciesDocuments,
        SpeciesThreats,
    },
    props: {
        existingSpeciesCombineList: {
            type: Array,
            required: true,
        },
        resultingSpecies: {
            type: Object,
            default: null,
        },
    },
    emits: ['resulting-species-changed', 'resulting-species-taxonomy-changed'],
    data: function () {
        return {
            select_scientific_name: 'species-combine-select-scientific-name',
            scientific_name_lookup: 'species-combine-scientific-name-lookup',
            profileTabId: 'profile-tab-' + uuid(),
            documentTabId: 'document-tab-' + uuid(),
            threatTabId: 'threat-tab-' + uuid(),
            originalSpecies: this.existingSpeciesCombineList?.[0] || null,
            selectedSpeciesIndex: 0,
        };
    },
    mounted: function () {
        this.initialiseScientificNameLookup();
        this.addTabShownEvents();
    },
    methods: {
        initialiseScientificNameLookup: function () {
            let vm = this;
            $(vm.$refs[vm.scientific_name_lookup])
                .select2({
                    minimumInputLength: 2,
                    dropdownParent: $('#' + vm.select_scientific_name),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Scientific Name',
                    ajax: {
                        url: api_endpoints.scientific_name_lookup,
                        dataType: 'json',
                        data: function (params) {
                            var query = {
                                term: params.term,
                                type: 'public',
                                group_type_id: vm.originalSpecies.group_type_id,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:select', function (e) {
                    swal.fire({
                        title: 'Confirm Taxonomy Change?',
                        text: 'Changing the resulting species taxonomy will result in any data you have entered below being overwritten.',
                        icon: 'question',
                        showCancelButton: true,
                        confirmButtonText: 'Confirm Taxonomy Change',
                        reverseButtons: true,
                        customClass: {
                            confirmButton: 'btn btn-primary',
                            cancelButton: 'btn btn-secondary',
                        },
                    }).then((result) => {
                        if (!result.isConfirmed) {
                            $(vm.$refs[vm.scientific_name_lookup])
                                .val(null)
                                .trigger('change');
                        } else {
                            let speciesId = e.params.data.species_id;
                            let taxonomyId = e.params.data.id;
                            vm.selectedSpeciesIndex = null;
                            vm.$emit(
                                'resulting-species-taxonomy-changed',
                                taxonomyId,
                                speciesId
                            );
                        }
                    });
                })
                .on('select2:unselect', function () {
                    vm.selectedSpeciesIndex = 0;
                    vm.$emit(
                        'resulting-species-taxonomy-changed',
                        vm.originalSpecies.taxonomy_id,
                        vm.originalSpecies.id
                    );
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.scientific_name_lookup +
                            '-results"]'
                    );
                    searchField[0].focus();
                });
        },
        beforeChangeSelectedSpecies: function (event, index) {
            if (this.selectedSpeciesIndex !== index) {
                swal.fire({
                    title: 'Confirm Taxonomy Change?',
                    text: 'Changing the resulting species taxonomy will result in any data you have entered below being overwritten.',
                    icon: 'question',
                    showCancelButton: true,
                    confirmButtonText: 'Confirm Taxonomy Change',
                    reverseButtons: true,
                    customClass: {
                        confirmButton: 'btn btn-primary',
                        cancelButton: 'btn btn-secondary',
                    },
                }).then((result) => {
                    if (result.isConfirmed) {
                        this.selectedSpeciesIndex = index;
                        $(this.$refs[this.scientific_name_lookup])
                            .val(null)
                            .trigger('change');
                        this.$emit(
                            'resulting-species-taxonomy-changed',
                            this.existingSpeciesCombineList[index].taxonomy_id,
                            this.existingSpeciesCombineList[index].id
                        );
                    } else {
                        event.preventDefault();
                    }
                });
            }
        },
        addTabShownEvents: function () {
            document
                .querySelectorAll('a[data-bs-toggle="pill"]')
                .forEach((el) => {
                    el.addEventListener('shown.bs.tab', () => {
                        if (el.id == 'pills-combine-threats-tab') {
                            this.$refs.species_combine_threats.forEach(
                                (component) => component.adjust_table_width()
                            );
                        } else if (el.id == 'pills-combine-documents-tab') {
                            this.$refs.species_combine_documents.forEach(
                                (component) => component.adjust_table_width()
                            );
                        }
                    });
                });
        },
    },
};
</script>
