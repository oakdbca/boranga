<template lang="html">
    <div id="species_combine_profile">
        <FormSection
            :form-collapse="false"
            label="Taxonomy"
            Index="species-combine-taxon-form-section-index"
        >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"></label>
                <div class="col-sm-8">
                    <textarea
                        id="species_display"
                        v-model="species_display"
                        disabled
                        class="form-control"
                        rows="3"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Common Name:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="common_name"
                        v-model="common_name"
                        :disabled="true"
                        class="form-control"
                        rows="2"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Taxon Name ID:</label
                >
                <div class="col-sm-8">
                    <input
                        id="species_combine_taxon_name_id"
                        v-model="taxon_name_id"
                        :disabled="true"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Previous Name:</label
                >
                <div class="col-sm-8">
                    <input
                        id="previous_name"
                        v-model="taxon_previous_name"
                        :disabled="true"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Phylogenetic Group:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="phylogenetic_group"
                        v-model="phylogenetic_group"
                        :disabled="true"
                        class="form-control"
                        rows="1"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Family:</label>
                <div class="col-sm-8">
                    <textarea
                        id="family"
                        v-model="family"
                        :disabled="true"
                        rows="1"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Genus:</label>
                <div class="col-sm-8">
                    <textarea
                        id="genus"
                        v-model="genus"
                        :disabled="true"
                        rows="1"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Name Authority:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="name_authority"
                        v-model="name_authority"
                        :disabled="true"
                        rows="1"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Name names comments:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="comment"
                        v-model="name_comments"
                        :disabled="true"
                        class="form-control"
                        rows="3"
                        placeholder=""
                    />
                </div>
            </div>
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="Distribution"
            Index="species-combine-distribution-form-section-index"
        >
            <div class="row mb-3">
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} Distribution:</label
                    >
                    <div class="col-sm-8">
                        <textarea
                            v-model="species.distribution.distribution"
                            :disabled="true"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                    <div class="col-sm-1">
                        <input
                            :id="'distribution' + species.id"
                            class="form-check-input"
                            type="checkbox"
                            name="distribution_chk"
                            @change="
                                checkDistributionInput(
                                    'distribution_chk',
                                    'distribution' + species.id,
                                    'distribution',
                                    species.distribution.distribution
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Distribution:</label
                >
                <div class="col-sm-9">
                    <textarea
                        id="distribution"
                        v-model="species_community.distribution.distribution"
                        class="form-control"
                        rows="1"
                        placeholder=""
                    />
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} Region:</label
                    >
                    <div class="col-sm-8">
                        <label for="" class="control-label">{{
                            getselectedRegionNames(species)
                        }}</label>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_id , v-model object attribute of this field) -->
                        <input
                            :id="'regions_select_chk' + species.id"
                            class="form-check-input"
                            type="checkbox"
                            @change="
                                checkRegionDistrictInput(
                                    'regions_select_chk' + species.id,
                                    'regions',
                                    species,
                                    'regions_select'
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Region:</label>
                <div :id="select_regions" class="col-sm-8">
                    <select
                        ref="regions_select"
                        v-model="species_community.regions"
                        style="width: 100%"
                        class="form-select input-sm"
                    >
                        <option value="" selected disabled>
                            Select region
                        </option>
                        <option
                            v-for="option in region_list"
                            :key="option.value"
                            :value="option.value"
                        >
                            {{ option.text }}
                        </option>
                    </select>
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} District:</label
                    >
                    <div class="col-sm-8">
                        <label for="" class="control-label">{{
                            getselectedDistrictNames(species)
                        }}</label>
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_id , v-model object attribute of this field) -->
                        <input
                            :id="'districts_select_chk' + species.id"
                            class="form-check-input"
                            type="checkbox"
                            @change="
                                checkRegionDistrictInput(
                                    'districts_select_chk' + species.id,
                                    'districts',
                                    species,
                                    'districts_select'
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">District:</label>
                <div :id="select_districts" class="col-sm-8">
                    <select
                        ref="districts_select"
                        v-model="species_community.districts"
                        class="form-select"
                    >
                        <option value="" selected disabled>
                            Select district
                        </option>
                        <option
                            v-for="option in district_list"
                            :key="option.value"
                            :value="option.value"
                        >
                            {{ option.text }}
                        </option>
                    </select>
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} Number of
                        Occurrences:</label
                    >
                    <div class="col-sm-8">
                        <input
                            id="no_of_occurrences_orig"
                            v-model="species.distribution.number_of_occurrences"
                            :disabled="true"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Number of Occurrences:</label
                >
                <div class="col-sm-6">
                    <input
                        id="no_of_occurrences"
                        v-model="
                            species_community.distribution.number_of_occurrences
                        "
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3">
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.noo_auto"
                            type="radio"
                            value="true"
                            class="noo_auto form-check-input"
                            name="noo_auto"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.noo_auto"
                            type="radio"
                            value="false"
                            class="noo_auto form-check-input"
                            name="noo_auto"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} Extent of Occurrences
                        (km2):</label
                    >
                    <div class="col-sm-8">
                        <input
                            id="extent_of_occurrence_orig"
                            v-model="species.distribution.extent_of_occurrences"
                            :disabled="true"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Extent of Occurrence (km2):</label
                >
                <div class="col-sm-6">
                    <input
                        id="extent_of_occurrence"
                        v-model="
                            species_community.distribution.extent_of_occurrences
                        "
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3">
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.eoo_auto"
                            type="radio"
                            value="true"
                            class="eoo_auto form-check-input"
                            name="eoo_auto"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.eoo_auto"
                            type="radio"
                            value="false"
                            class="eoo_auto form-check-input"
                            name="eoo_auto"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} Actual Area of
                        Occupancy<br />(km2):</label
                    >
                    <div class="col-sm-8">
                        <input
                            id="area_of_occupancy_actual_orig"
                            v-model="
                                species.distribution.area_of_occupancy_actual
                            "
                            :disabled="true"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Actual Area of Occupancy<br />(km2):</label
                >
                <div class="col-sm-6">
                    <input
                        id="area_of_occupancy_actual"
                        v-model="
                            species_community.distribution
                                .area_of_occupancy_actual
                        "
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3">
                    <div class="form-check form-check-inline">
                        <input
                            v-model="
                                species_community.distribution.aoo_actual_auto
                            "
                            type="radio"
                            value="true"
                            class="aoo_actual_auto form-check-input"
                            name="aoo_actual_auto"
                        />
                        <label class="form-check-label">auto</label>
                    </div>
                    <div class="form-check form-check-inline">
                        <input
                            v-model="
                                species_community.distribution.aoo_actual_auto
                            "
                            type="radio"
                            value="false"
                            class="aoo_actual_auto form-check-input"
                            name="aoo_actual_auto"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} Area of Occupancy<br />(km2):</label
                    >
                    <div class="col-sm-8">
                        <input
                            id="area_of_occupany_orig"
                            v-model="species.distribution.area_of_occupancy"
                            :disabled="true"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Area of Occupancy<br />(10km x 10km):</label
                >
                <div class="col-sm-6">
                    <input
                        id="area_of_occupany"
                        v-model="
                            species_community.distribution.area_of_occupancy
                        "
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} Number of IUCN
                        Locations:</label
                    >
                    <div class="col-sm-8">
                        <input
                            id="no_of_iucn_locations"
                            v-model="
                                species.distribution.number_of_iucn_locations
                            "
                            :disabled="true"
                            type="number"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Number of IUCN Locations:</label
                >
                <div class="col-sm-8">
                    <input
                        id="no_of_iucn_locations"
                        v-model="
                            species_community.distribution
                                .number_of_iucn_locations
                        "
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
        </FormSection>
        <FormSection
            :form-collapse="false"
            label="General"
            Index="species-combine-general-form-section-index"
        >
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} Department File
                        Numbers:</label
                    >
                    <div class="col-sm-8">
                        <input
                            id="department_file_numbers"
                            v-model="species.department_file_numbers"
                            :disabled="true"
                            type="text"
                            class="form-control"
                            placeholder=""
                        />
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field) -->
                        <input
                            :id="'dept_file_chk' + species.id"
                            class="form-check-input"
                            type="checkbox"
                            name="dept_file_chk"
                            @change="
                                checkDistributionInput(
                                    'dept_file_chk',
                                    'dept_file_chk' + species.id,
                                    'department_file_numbers',
                                    species.department_file_numbers
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Department File Numbers:</label
                >
                <div class="col-sm-8">
                    <input
                        id="department_file_numbers"
                        v-model="species_community.department_file_numbers"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>

            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Last data curation date:
                </label>
                <div class="col-sm-8">
                    <input
                        ref="last_data_curation_date"
                        v-model="species_community.last_data_curation_date"
                        type="date"
                        class="form-control"
                        name="last_data_curation_date"
                        min="1990-01-01"
                        :max="new Date().toISOString().split('T')[0]"
                        @blur="checkDate()"
                    />
                </div>
            </div>
            <div>
                <div
                    v-for="species in speciesBeingCombined"
                    :key="species.id"
                    class="row mb-3"
                >
                    <label for="" class="col-sm-3 control-label"
                        >{{ species.species_number }} Comment:</label
                    >
                    <div class="col-sm-8">
                        <textarea
                            id="comment"
                            v-model="species.comment"
                            :disabled="true"
                            class="form-control"
                            rows="3"
                            placeholder=""
                        />
                    </div>
                    <div class="col-sm-1">
                        <!-- checkInput(checkbox_name,checkbox_id , v-model object attribute of this field, value) -->
                        <input
                            :id="'comment_chk' + species.id"
                            class="form-check-input"
                            type="checkbox"
                            name="comment_chk"
                            @change="
                                checkCommentInput(
                                    'comment_chk',
                                    'comment_chk' + species.id,
                                    'comment',
                                    species.comment
                                )
                            "
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Comment:</label>
                <div class="col-sm-8">
                    <textarea
                        id="comment"
                        v-model="species_community.comment"
                        class="form-control"
                        rows="3"
                        placeholder=""
                    />
                </div>
            </div>
        </FormSection>
    </div>
</template>

<script>
import FormSection from '@/components/forms/section_toggle.vue';
import { api_endpoints } from '@/utils/hooks';
export default {
    name: 'SpeciesCombineProfile',
    components: {
        FormSection,
    },
    props: {
        species_community: {
            type: Object,
            required: true,
        },
        speciesBeingCombined: {
            type: Array,
            required: true,
        },
    },
    data: function () {
        return {
            select_regions: 'species-combine-select-regions',
            select_districts: 'species-combine-select-districts',

            region_list: [],
            district_list: [],

            selectedSpecies: null,
            species_display: '',
            common_name: null,
            taxon_name_id: null,
            taxon_previous_name: null,
            phylogenetic_group: null,
            family: null,
            genus: null,
            name_authority: null,
            name_comments: null,
        };
    },
    watch: {
        'species_community.distribution.noo_auto': function (newVal) {
            let vm = this;
            var selectedValue = newVal;
            if (selectedValue === 'true') {
                vm.species_community.distribution.number_of_occurrences =
                    vm.species_community.distribution.cal_number_of_occurrences;
            } else {
                vm.species_community.distribution.number_of_occurrences = null;
            }
        },
        'species_community.distribution.eoo_auto': function (newVal) {
            let vm = this;
            var selectedValue = newVal;
            if (selectedValue === 'true') {
                vm.species_community.distribution.extent_of_occurrences =
                    vm.species_community.distribution.cal_extent_of_occurrences;
            } else {
                vm.species_community.distribution.extent_of_occurrences = null;
            }
        },
        'species_community.distribution.aoo_actual_auto': function (newVal) {
            let vm = this;
            var selectedValue = newVal;
            if (selectedValue === 'true') {
                vm.species_community.distribution.area_of_occupancy_actual =
                    vm.species_community.distribution.cal_area_of_occupancy_actual;
            } else {
                vm.species_community.distribution.area_of_occupancy_actual =
                    null;
            }
        },
    },
    created: async function () {
        let vm = this;
        //----set the distribution field values if auto onload
        if (vm.species_community.distribution.noo_auto == true) {
            vm.species_community.distribution.number_of_occurrences =
                vm.species_community.distribution.cal_number_of_occurrences;
        }
        if (vm.species_community.distribution.eoo_auto == true) {
            vm.species_community.distribution.extent_of_occurrences =
                vm.species_community.distribution.cal_extent_of_occurrences;
        }
        if (vm.species_community.distribution.aoo_actual_auto == true) {
            vm.species_community.distribution.area_of_occupancy_actual =
                vm.species_community.distribution.cal_area_of_occupancy_actual;
        }

        vm.fetchRegions();
    },
    mounted: function () {
        let vm = this;
        vm.loadTaxonomydetails();
        vm.initialiseRegionSelect();
        vm.initialiseDistrictSelect();
    },
    methods: {
        checkDate: function () {
            if (this.species_community.last_data_curation_date === '') {
                this.species_community.last_data_curation_date = null;
                return;
            }
            if (
                this.species_community.last_data_curation_date === null ||
                isNaN(new Date(this.species_community.last_data_curation_date))
            ) {
                return;
            }
            if (
                new Date(this.species_community.last_data_curation_date) >
                new Date()
            ) {
                this.species_community.last_data_curation_date = new Date()
                    .toISOString()
                    .split('T')[0];
                this.$nextTick(() => {
                    this.$refs.last_data_curation_date.focus();
                });
                swal.fire({
                    title: 'Error',
                    text: 'Last data curation date cannot be in the future',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
            if (
                new Date(this.species_community.last_data_curation_date) <
                new Date('1990-01-01')
            ) {
                this.species_community.last_data_curation_date = new Date(
                    '1990-01-01'
                )
                    .toISOString()
                    .split('T')[0];
                this.$nextTick(() => {
                    this.$refs.last_data_curation_date.focus();
                });
                swal.fire({
                    title: 'Error',
                    text: 'Last data curation date cannot be before 01/01/1990',
                    icon: 'error',
                    customClass: {
                        confirmButton: 'btn btn-primary',
                    },
                });
            }
        },
        loadTaxonomydetails: function () {
            let vm = this;
            if (vm.species_community.taxonomy_details != null) {
                var newOption = new Option(
                    vm.species_community.taxonomy_details.scientific_name,
                    vm.species_community.taxonomy_id,
                    false,
                    true
                );
                $('#' + vm.scientific_name_lookup).append(newOption);
                vm.species_display =
                    vm.species_community.taxonomy_details.scientific_name;
                vm.species_display =
                    vm.species_community.taxonomy_details.scientific_name;
                vm.common_name =
                    vm.species_community.taxonomy_details.common_name;
                vm.taxon_name_id =
                    vm.species_community.taxonomy_details.taxon_name_id;
                vm.taxon_previous_name =
                    vm.species_community.taxonomy_details.taxon_previous_name;
                vm.phylogenetic_group =
                    vm.species_community.taxonomy_details.phylogenetic_group;
                vm.family = vm.species_community.taxonomy_details.family_name;
                vm.genus = vm.species_community.taxonomy_details.genera_name;
                vm.name_authority =
                    vm.species_community.taxonomy_details.name_authority;
                vm.name_comments =
                    vm.species_community.taxonomy_details.name_comments;
            }
        },
        checkDistributionInput: function (
            chkbox_name,
            chkbox_id,
            obj_field,
            value
        ) {
            // if checkbox is checked copy value from original  species to new species
            if ($('#' + chkbox_id).is(':checked') == true) {
                this.species_community.distribution[obj_field] = value;
            } else {
                this.species_community.distribution[obj_field] = null;
            }
            //--- to select only one checkbox at a time in a group
            let chkbox_name_arr = document.getElementsByName(chkbox_name);
            for (var i = 0; i < chkbox_name_arr.length; i++) {
                if (chkbox_name_arr[i].id != chkbox_id) {
                    chkbox_name_arr[i].checked = false;
                }
            }
        },
        checkCommentInput: function (chkbox_name, chkbox_id, obj_field, value) {
            // if checkbox is checked copy value from original  species to new species
            if ($('#' + chkbox_id).is(':checked') == true) {
                this.species_community[obj_field] = value;
            } else {
                this.species_community[obj_field] = null;
            }
            //--- to select only one checkbox at a time in a group
            let chkbox_name_arr = document.getElementsByName(chkbox_name);
            for (var i = 0; i < chkbox_name_arr.length; i++) {
                if (chkbox_name_arr[i].id != chkbox_id) {
                    chkbox_name_arr[i].checked = false;
                }
            }
        },
        getselectedRegionNames(species) {
            // Filter regions_list to get only the selected regions
            let selected_region = species.regions;
            const selectedRegions = this.region_list.filter((region) =>
                selected_region.includes(region.value)
            );
            // Map the selected regions to their names and join them with commas
            return selectedRegions.map((region) => region.text).join(', ');
        },
        getselectedDistrictNames(species) {
            // Initialize an empty array to store the names of selected districts
            let selectedNames = [];
            // Iterate over each region
            this.region_list.forEach((region) => {
                // Filter the districts of the current region
                region.districts.forEach((district) => {
                    if (species.districts.includes(district.id)) {
                        selectedNames.push(district.name);
                    }
                });
            });
            // Join the names with commas
            return selectedNames.join(', ');
        },
        checkRegionDistrictInput: function (
            chkbox,
            obj_field,
            species,
            select2_ref = ''
        ) {
            // if checkbox is checked copy value from original  species to new species
            if ($('#' + chkbox).is(':checked') == true) {
                this.species_community[obj_field] = species[obj_field];
                if (select2_ref != '') {
                    $(this.$refs[select2_ref])
                        .val(this.species_community[obj_field])
                        .trigger('change');
                    this.chainedSelectDistricts(
                        this.species_community.regions,
                        'select'
                    );
                    this.species_community[obj_field] = species[obj_field];
                }
            } else {
                if (select2_ref != '') {
                    $(this.$refs[select2_ref]).val('').trigger('change');
                    this.chainedSelectDistricts(
                        this.species_community.regions,
                        'deselect'
                    );
                }
                this.species_community[obj_field] = [];
            }
        },
        fetchRegions: function () {
            let vm = this;

            fetch(api_endpoints.regions).then(
                async (response) => {
                    vm.api_regions = await response.json();
                    for (var i = 0; i < vm.api_regions.length; i++) {
                        this.region_list.push({
                            text: vm.api_regions[i].name,
                            value: vm.api_regions[i].id,
                            districts: vm.api_regions[i].districts,
                        });
                    }
                    // vm.setProposalData2(this.regions);
                    if (vm.species_community.regions) {
                        vm.chainedSelectDistricts(
                            vm.species_community.regions,
                            'fetch'
                        );
                    }
                },
                (error) => {
                    console.log(error);
                }
            );
        },
        searchList: function (id, search_list) {
            for (var i = 0; i < search_list.length; i++) {
                if (search_list[i].value == id) {
                    return search_list[i];
                }
            }
            return { districts: [] };
        },
        chainedSelectDistricts: function (regions, action) {
            let vm = this;
            if (action != 'fetch') {
                vm.species_community.districts = []; //-----to remove the previous selection
            }
            vm.district_list = [];
            if (regions) {
                for (let r of regions) {
                    var api_districts = this.searchList(
                        r,
                        vm.region_list
                    ).districts;
                    if (api_districts.length > 0) {
                        for (var i = 0; i < api_districts.length; i++) {
                            this.district_list.push({
                                text: api_districts[i].name,
                                value: api_districts[i].id,
                            });
                        }
                    }
                }
            }
        },
        initialiseRegionSelect: function () {
            let vm = this;
            $(vm.$refs.regions_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    multiple: true,
                    dropdownParent: $('#' + vm.select_regions),
                    placeholder: 'Select Region',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.regions = selected.val();
                    vm.chainedSelectDistricts(
                        vm.species_community.regions,
                        'select'
                    );
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.regions = selected.val();
                    vm.chainedSelectDistricts(
                        vm.species_community.regions,
                        'deselect'
                    );
                });
        },
        initialiseDistrictSelect: function () {
            let vm = this;
            $(vm.$refs.districts_select)
                .select2({
                    theme: 'bootstrap-5',
                    allowClear: true,
                    dropdownParent: $('#' + vm.select_districts),
                    multiple: true,
                    placeholder: 'Select District',
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.districts = selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.districts = selected.val();
                });
        },
    },
};
</script>
