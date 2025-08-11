<template lang="html">
    <div id="combine-species">
        <modal
            id="species-combine-modal"
            transition="modal fade"
            :title="title"
            extra-large
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="combine-species-form">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div>
                            <div class="col-md-12">
                                <ul
                                    id="combine-pills-tab"
                                    class="nav nav-pills"
                                    role="tablist"
                                >
                                    <li
                                        v-for="(
                                            species, index
                                        ) in speciesCombineList"
                                        :key="'li' + index"
                                        class="nav-item"
                                    >
                                        <a
                                            :id="
                                                'pills-species-' +
                                                index +
                                                '-tab'
                                            "
                                            class="nav-link small py-2"
                                            data-bs-toggle="pill"
                                            :data-bs-target="
                                                '#species-body-' + index
                                            "
                                            :href="'#species-body-' + index"
                                            role="tab"
                                            :aria-controls="
                                                'species-body-' + index
                                            "
                                            aria-selected="false"
                                        >
                                            Combine {{ index + 1
                                            }}<span
                                                v-if="index > 0"
                                                :id="index"
                                                class="ms-2"
                                                @click.prevent="
                                                    removeCombineSpecies(
                                                        species
                                                    )
                                                "
                                                ><i
                                                    class="bi bi-trash3-fill"
                                                ></i
                                            ></span>
                                            <!-- can delete the original species except the current original species , so check index>0 -->
                                        </a>
                                    </li>
                                    <li class="nav-item me-2">
                                        <a
                                            id="combineSpeciesBtnAdd"
                                            class="nav-link small py-2"
                                            href="#"
                                            @click.prevent="
                                                addSpeciesToCombine()
                                            "
                                            ><i class="bi bi-window-plus"></i>
                                            Add</a
                                        >
                                    </li>
                                    <li class="nav-item me-2">
                                        <a
                                            id="pills-resulting-species-tab"
                                            class="nav-link small py-2"
                                            data-bs-toggle="pill"
                                            data-bs-target="#resulting-species"
                                            href="#resulting-species"
                                            role="tab"
                                            aria-controls="resulting-species"
                                            aria-selected="true"
                                        >
                                            Resulting Species
                                        </a>
                                    </li>
                                    <li class="nav-item">
                                        <a
                                            id="finalise-combine"
                                            class="nav-link small py-2"
                                            data-bs-toggle="pill"
                                            data-bs-target="#finalise-combine-tab-pane"
                                            href="#finalise-combine-tab-pane"
                                            role="tab"
                                            aria-controls="finalise-combine-tab-pane"
                                            aria-selected="false"
                                            ><i class="bi bi-check2-circle"></i>
                                            Finalise Combine</a
                                        >
                                    </li>
                                </ul>
                                <div
                                    id="combine-pills-tabContent"
                                    class="tab-content border p-3"
                                >
                                    <div
                                        v-for="(
                                            species, index
                                        ) in speciesCombineList"
                                        :id="'species-body-' + index"
                                        :key="'div' + index"
                                        class="tab-pane fade"
                                        role="tabpanel"
                                        :aria-labelledby="
                                            'pills-species' + index + '-tab'
                                        "
                                    >
                                        <FormSpeciesCommunities
                                            v-if="species"
                                            :key="'species-' + index"
                                            :id="'species-' + index"
                                            :ref="
                                                'species_communities_species' +
                                                index
                                            "
                                            :species_community="species"
                                            :species_community_original="
                                                species_community
                                            "
                                            :is_internal="true"
                                            :is_readonly="true"
                                        >
                                        </FormSpeciesCommunities>
                                    </div>
                                    <div
                                        id="resulting-species"
                                        class="tab-pane"
                                        role="tabpanel"
                                        aria-labelledby="pills-resulting-species-tab"
                                    >
                                        <FormSpeciesCombine
                                            v-if="
                                                speciesCombineList &&
                                                resultingSpecies
                                            "
                                            id="resulting-species"
                                            :resulting-species="
                                                resultingSpecies
                                            "
                                            :existing-species-combine-list="
                                                existingSpeciesCombineList
                                            "
                                            :is_internal="true"
                                            @resulting-species-taxonomy-changed="
                                                resultingSpeciesTaxonomyChanged
                                            "
                                        >
                                        </FormSpeciesCombine>
                                    </div>
                                    <div
                                        v-if="
                                            resultingSpecies &&
                                            speciesCombineList &&
                                            speciesCombineList.length > 0
                                        "
                                        id="finalise-combine-tab-pane"
                                        class="tab-pane"
                                        role="tabpanel"
                                        aria-labelledby="finalise-combine"
                                    >
                                        <p class="border-bottom mb-3">
                                            <HelpText
                                                section_id="species_combine_finalise"
                                            />
                                        </p>

                                        <p>
                                            You are about to combine the
                                            following species:
                                        </p>

                                        <div class="mb-3">
                                            <li
                                                v-for="species in speciesCombineList"
                                                :key="species.id"
                                                class="text-secondary mb-3"
                                            >
                                                <span
                                                    class="badge bg-light text-primary text-capitalize border p-2 fs-6 me-2"
                                                    >{{
                                                        species.species_number
                                                    }}
                                                    -
                                                    {{
                                                        species.taxonomy_details
                                                            .scientific_name
                                                    }}</span
                                                >
                                            </li>
                                        </div>

                                        <p>
                                            Into the
                                            <template v-if="resultingSpecies.id"
                                                >existing</template
                                            ><template v-else>new</template>
                                            species:
                                        </p>

                                        <div class="border-bottom mb-3 pb-3">
                                            <span
                                                class="badge bg-light text-primary text-capitalize border p-2 fs-6"
                                                ><template
                                                    v-if="
                                                        resultingSpecies.species_number
                                                    "
                                                    >{{
                                                        resultingSpecies.species_number
                                                    }}</template
                                                >
                                                <template
                                                    v-if="
                                                        resultingSpecies.taxonomy_details &&
                                                        resultingSpecies
                                                            .taxonomy_details
                                                            .scientific_name
                                                    "
                                                    >-
                                                    {{
                                                        resultingSpecies
                                                            .taxonomy_details
                                                            .scientific_name
                                                    }}</template
                                                ></span
                                            >
                                        </div>

                                        <button
                                            class="button btn btn-primary"
                                            :disabled="finaliseCombineLoading"
                                            @click.prevent="validateForm()"
                                        >
                                            <i class="bi bi-check2-circle"></i>
                                            Finalise Combine
                                            <template
                                                v-if="finaliseCombineLoading"
                                                ><span
                                                    class="spinner-border spinner-border-sm"
                                                    role="status"
                                                    aria-hidden="true"
                                                ></span>
                                                <span class="visually-hidden"
                                                    >Loading...</span
                                                ></template
                                            >
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </form>
                </div>
            </div>

            <template #footer>
                <div>
                    <button
                        type="button"
                        class="btn btn-secondary me-2"
                        @click="cancel"
                    >
                        Cancel
                    </button>
                </div>
            </template>
        </modal>

        <AddCombineSpecies ref="addCombineSpecies"></AddCombineSpecies>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
import FormSpeciesCommunities from '@/components/form_species_communities.vue';
import FormSpeciesCombine from '@/components/form_species_combine.vue';
import AddCombineSpecies from '@/components/common/species_communities/species_combine/add_combine_species.vue';
import HelpText from '@/components/common/help_text.vue';

import { helpers, api_endpoints } from '@/utils/hooks.js';

export default {
    name: 'SpeciesCombine',
    components: {
        modal,
        alert,
        FormSpeciesCommunities,
        FormSpeciesCombine,
        AddCombineSpecies,
        HelpText,
    },
    props: {
        species_community: {
            type: Object,
            required: true,
        },
    },
    data: function () {
        return {
            resultingSpecies: null,
            submittingSpeciesCombine: false,
            isModalOpen: false,
            speciesCombineList: [],
            finaliseCombineLoading: false,
            errorString: '',
        };
    },
    computed: {
        title: function () {
            return `Combine Species - ${this.species_community.species_number} - ${this.species_community.taxonomy_details.scientific_name}`;
        },
        existingSpeciesCombineList: function () {
            return this.speciesCombineList.filter(
                (species) => species.id !== null
            );
        },
        combiningOnlyIntoSelf: function () {
            return (
                this.speciesCombineList.length === 1 &&
                this.speciesCombineList[0].id === this.species_community.id
            );
        },
    },
    watch: {
        isModalOpen: function (val) {
            if (val) {
                this.$nextTick(() => {
                    // was added to set the first species Tab active but the updated() method overrides it
                    var firstTabEl = document.querySelector(
                        '#combine-pills-tab li:nth-child(1) a'
                    );
                    var firstTab =
                        bootstrap.Tab.getOrCreateInstance(firstTabEl);
                    firstTab.show();
                });
            }
        },
    },
    created: function () {
        this.addSpeciesObjectToCombineList(this.species_community);
        this.resultingSpecies = JSON.parse(
            JSON.stringify(this.species_community)
        );
        this.resultingSpecies.documents = [];
        this.resultingSpecies.threats = [];
        this.resultingSpecies.documentsThreatsSelection = {
            documents: {},
            threats: {},
        };
    },
    mounted: function () {
        this.beforeShowResultingSpeciesTab();
        this.beforeShowFinaliseTab();
    },
    methods: {
        validateForm: function () {
            if ($(document.forms['combine-species-form']).valid()) {
                this.combineSpecies();
            }
        },
        cancel: function () {
            swal.fire({
                title: 'Are you sure you want to close this modal?',
                text: 'You will lose any unsaved changes.',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Yes, close it',
                cancelButtonText: 'Return to modal',
                reverseButtons: true,
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
            }).then((result) => {
                if (result.isConfirmed) {
                    this.close();
                }
            });
        },
        close: function () {
            this.isModalOpen = false;
            this.errorString = '';
        },
        can_submit: function () {
            let vm = this;
            let blank_fields = [];
            if (
                vm.resultingSpecies.taxonomy_id == null ||
                vm.resultingSpecies.taxonomy_id == ''
            ) {
                blank_fields.push(
                    ' Species ' +
                        vm.resultingSpecies.species_number +
                        ' Scientific Name is missing'
                );
            }
            if (
                vm.resultingSpecies.distribution.distribution == null ||
                vm.resultingSpecies.distribution.distribution == ''
            ) {
                blank_fields.push(' Distribution is missing');
            }
            if (
                vm.resultingSpecies.regions == null ||
                vm.resultingSpecies.regions == '' ||
                vm.resultingSpecies.regions.length == 0
            ) {
                blank_fields.push(' Region is missing');
            }
            if (
                vm.resultingSpecies.districts == null ||
                vm.resultingSpecies.districts == '' ||
                vm.resultingSpecies.districts.length == 0
            ) {
                blank_fields.push(' District is missing');
            }
            if (blank_fields.length == 0) {
                return true;
            } else {
                return blank_fields;
            }
        },
        combineSpecies: async function () {
            let vm = this;
            var missing_data = vm.can_submit();
            if (missing_data != true) {
                await swal.fire({
                    title: 'Please fix following errors before submitting',
                    text: missing_data,
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
                // Show the resulting species tab
                let resultingSpeciesTab = document.querySelector(
                    '#pills-resulting-species-tab'
                );
                let resultingSpeciesTabInstance =
                    bootstrap.Tab.getOrCreateInstance(resultingSpeciesTab);
                resultingSpeciesTabInstance.show();
                return;
            }

            vm.submittingSpeciesCombine = true;
            swal.fire({
                title: 'Combine Species',
                text: 'Are you sure you want to combine those species?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Combine Species',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            })
                .then(
                    async (swalresult) => {
                        if (swalresult.isConfirmed) {
                            vm.finaliseCombineLoading = true;
                            let payload = {
                                resulting_species: vm.resultingSpecies,
                                species_combine_list: vm.speciesCombineList,
                                documents_threats_selection:
                                    vm.resultingSpecies
                                        .documentsThreatsSelection,
                            };
                            let submit_url = helpers.add_endpoint_json(
                                api_endpoints.species,
                                vm.speciesCombineList[0].id + '/combine_species'
                            );
                            fetch(submit_url, {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                },
                                body: JSON.stringify(payload),
                            }).then(
                                async (response) => {
                                    vm.new_species = await response.json();
                                    vm.$router.push({
                                        name: 'internal-species-communities-dash',
                                    });
                                },
                                (err) => {
                                    swal.fire({
                                        title: 'Submit Error',
                                        text: helpers.apiVueResourceError(err),
                                        icon: 'error',
                                        customClass: {
                                            confirmButton: 'btn btn-primary',
                                        },
                                    });
                                }
                            );
                        }
                    },
                    (error) => {
                        console.log(error);
                    }
                )
                .finally(() => {
                    vm.finaliseCombineLoading = false;
                });
        },
        removeCombineSpecies: function (species) {
            let vm = this;
            swal.fire({
                title: 'Remove Species',
                text: `Are you sure you want to remove '${species.taxonomy_details.scientific_name}' from this combine?`,
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Remove Species',
                customClass: {
                    confirmButton: 'btn btn-primary',
                    cancelButton: 'btn btn-secondary',
                },
                reverseButtons: true,
            }).then(async (swalresult) => {
                if (swalresult.isConfirmed) {
                    let species_index = vm.speciesCombineList.indexOf(species);
                    vm.speciesCombineList.splice(species_index, 1);
                    this.$nextTick(() => {
                        vm.showLastCombineSpeciesTab();
                    });
                }
            });
        },
        addSpeciesToCombine: function () {
            this.$refs.addCombineSpecies.isModalOpen = true;
        },
        addSpeciesObjectToCombineList: function (speciesObject) {
            // Also called from AddCombineSpecies child component
            speciesObject.threat_ids_to_copy = [];
            speciesObject.document_ids_to_copy = [];
            this.speciesCombineList.push(speciesObject);
        },
        showLastCombineSpeciesTab: function () {
            this.$nextTick(() => {
                let lastIndex = this.speciesCombineList.length - 1;
                let lastTabEl = document.querySelector(
                    `#combine-pills-tab li:nth-child(${lastIndex + 1}) a`
                );
                let lastTab = bootstrap.Tab.getOrCreateInstance(lastTabEl);
                lastTab.show();
            });
        },
        resultingSpeciesTaxonomyChanged: function (taxonomyId, speciesId) {
            if (speciesId) {
                this.fetchSpeciesData(speciesId);
            } else {
                this.getEmptySpeciesObject(String(taxonomyId));
            }
        },
        fetchSpeciesData: function (speciesId) {
            fetch(helpers.add_endpoint_json(api_endpoints.species, speciesId))
                .then((response) => response.json())
                .then((data) => {
                    this.resultingSpecies = JSON.parse(JSON.stringify(data));
                })
                .catch((error) => {
                    console.error('Error fetching species data:', error);
                });
        },
        getEmptySpeciesObject: function (taxonomyId) {
            fetch(api_endpoints.get_empty_species_object(taxonomyId))
                .then((response) => response.json())
                .then((data) => {
                    this.resultingSpecies = JSON.parse(JSON.stringify(data));
                })
                .catch((error) => {
                    console.error(
                        'Error fetching empty species object data:',
                        error
                    );
                });
        },
        validateAtLeastTwoSpecies: function (event) {
            if (this.combiningOnlyIntoSelf) {
                event.preventDefault();
                swal.fire({
                    title: 'Cannot Combine with Self Only',
                    html: '<p>There is no point in combining a species into itself only.</p><p class="mb-0">Either add another species to combine with or just edit the species record directly.</p>',
                    icon: 'info',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
        },
        beforeShowResultingSpeciesTab: function () {
            let vm = this;
            // Add a bootstrap event before the resulting species tab is shown
            let tabEl = document.querySelector('#pills-resulting-species-tab');
            tabEl.addEventListener('show.bs.tab', function (event) {
                vm.validateAtLeastTwoSpecies(event);
            });
        },
        beforeShowFinaliseTab: function () {
            let vm = this;
            // Add a bootstrap event before the finalise tab is shown
            let tabEl = document.querySelector('#finalise-combine');
            tabEl.addEventListener('show.bs.tab', function (event) {
                vm.validateAtLeastTwoSpecies(event);
            });
        },
    },
};
</script>

<style scoped>
.bi.bi-trash3-fill:hover {
    cursor: pointer;
    color: red;
}
</style>
