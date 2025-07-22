<template lang="html">
    <div id="species">
        {{ species_original }}<br />
        species_community.regions
        {{ species_community.regions }}<br />
        species_community.districts
        {{ species_community.districts }}
        <FormSection :form-collapse="false" label="Taxonomy" :Index="taxonBody">
            <div v-if="showRetainOriginalSpeciesCheckbox" class="row mb-3">
                <div for="" class="col-sm-3">Retain Original Species?</div>
                <div class="col-sm-8">
                    <div class="form-check">
                        <input
                            class="form-check-input"
                            type="checkbox"
                            name="retain-original-species"
                            id="retain-original-species"
                            v-model="retainOriginalSpecies"
                            @change="toggleRetainOriginalSpecies"
                        />
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Scientific Name:</label
                >
                <div :id="select_scientific_name" class="col-sm-8">
                    <select
                        :id="scientific_name_lookup"
                        :ref="scientific_name_lookup"
                        :disabled="isReadOnly"
                        :name="scientific_name_lookup"
                        class="form-control"
                    />
                </div>
            </div>
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
                        id="taxon_name_id"
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
                    >Nomos names comments:</label
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
            v-if="!thisSplitSpeciesHasOriginalTaxonomyId"
            :form-collapse="false"
            label="Distribution"
            :Index="distributionBody"
        >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Distribution:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="distribution"
                        v-model="species_original.distribution.distribution"
                        :disabled="true"
                        class="form-control"
                        rows="1"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'distribution' + species_community.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'distribution' + species_community.id,
                                'distribution'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Distribution:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="distribution"
                        v-model="species_community.distribution.distribution"
                        :disabled="isReadOnly"
                        class="form-control"
                        rows="1"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Region:</label
                >
                <div :id="select_regions_read_only" class="col-sm-8">
                    <label for="" class="control-label">{{
                        getselectedRegionNames(species_original)
                    }}</label>
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'regions_select_chk' + species_community.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkRegionDistrictInput(
                                'regions_select_chk' + species_community.id,
                                'regions',
                                'regions_select'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Region:</label>
                <div :id="select_regions" class="col-sm-8">
                    <select
                        ref="regions_select"
                        v-model="species_community.regions"
                        :disabled="isReadOnly"
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
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} District:</label
                >
                <div :id="select_districts_read_only" class="col-sm-8">
                    <label for="" class="control-label">{{
                        getselectedDistrictNames(species_original)
                    }}</label>
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'districts_select_chk' + species_community.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkRegionDistrictInput(
                                'districts_select_chk' + species_community.id,
                                'districts',
                                'districts_select'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">District:</label>
                <div :id="select_districts" class="col-sm-8">
                    <select
                        ref="districts_select"
                        v-model="species_community.districts"
                        :disabled="isReadOnly"
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
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Number of
                    Occurrences:</label
                >
                <div class="col-sm-6">
                    <input
                        id="no_of_occurrences_orig"
                        v-model="
                            species_original.distribution.number_of_occurrences
                        "
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
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
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3">
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.noo_auto"
                            :disabled="isReadOnly"
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
                            :disabled="isReadOnly"
                            type="radio"
                            value="false"
                            class="noo_auto form-check-input"
                            name="noo_auto"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Extent of Occurrences
                    (km2):</label
                >
                <div class="col-sm-6">
                    <input
                        id="extent_of_occurrence_orig"
                        v-model="
                            species_original.distribution.extent_of_occurrences
                        "
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >Extent of Occurrences (km2):</label
                >
                <div class="col-sm-6">
                    <input
                        id="extent_of_occurrence"
                        v-model="
                            species_community.distribution.extent_of_occurrences
                        "
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-3">
                    <div class="form-check form-check-inline">
                        <input
                            v-model="species_community.distribution.eoo_auto"
                            :disabled="isReadOnly"
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
                            :disabled="isReadOnly"
                            type="radio"
                            value="false"
                            class="eoo_auto form-check-input"
                            name="eoo_auto"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Actual Area of
                    Occupancy<br />(km2):</label
                >
                <div class="col-sm-6">
                    <input
                        id="area_of_occupancy_actual_orig"
                        v-model="
                            species_original.distribution
                                .area_of_occupancy_actual
                        "
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
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
                        :disabled="isReadOnly"
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
                            :disabled="isReadOnly"
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
                            :disabled="isReadOnly"
                            type="radio"
                            value="false"
                            class="aoo_actual_auto form-check-input"
                            name="aoo_actual_auto"
                        />
                        <label class="form-check-label">manual</label>
                    </div>
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Area of Occupancy<br />(2km
                    x 2km):</label
                >
                <div class="col-sm-6">
                    <input
                        id="area_of_occupany_orig"
                        v-model="
                            species_original.distribution.area_of_occupancy
                        "
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
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
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Number of IUCN
                    Locations:</label
                >
                <div class="col-sm-8">
                    <input
                        id="no_of_iucn_locations"
                        v-model="
                            species_original.distribution
                                .number_of_iucn_locations
                        "
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'no_of_iucn_locations' + species_community.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'no_of_iucn_locations' + species_community.id,
                                'number_of_iucn_locations'
                            )
                        "
                    />
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
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 col-form-label"
                    >{{ species_original.species_number }} Number of IUCN
                    Sub-populations:</label
                >
                <div class="col-sm-8">
                    <input
                        id="number_of_iucn_subpopulations"
                        v-model="
                            species_original.distribution
                                .number_of_iucn_subpopulations
                        "
                        :disabled="true"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'no_of_iucn_subpopulations' + species_community.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'no_of_iucn_subpopulations' +
                                    species_community.id,
                                'number_of_iucn_subpopulations'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 col-form-label"
                    >Number of IUCN Sub-populations:</label
                >
                <div class="col-sm-8">
                    <input
                        id="number_of_iucn_subpopulations"
                        v-model="
                            species_community.distribution
                                .number_of_iucn_subpopulations
                        "
                        :disabled="isReadOnly"
                        type="number"
                        class="form-control"
                        placeholder=""
                    />
                </div>
            </div>
        </FormSection>
        <FormSection
            v-if="!thisSplitSpeciesHasOriginalTaxonomyId"
            :form-collapse="false"
            label="General"
            :Index="generalBody"
        >
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Department File
                    Numbers:</label
                >
                <div class="col-sm-8">
                    <input
                        id="department_file_numbers"
                        v-model="species_original.department_file_numbers"
                        :disabled="true"
                        type="text"
                        class="form-control"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'dept_file_chk' + species_community.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkDistributionInput(
                                'dept_file_chk' + species_community.id,
                                'department_file_numbers'
                            )
                        "
                    />
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
                        :disabled="isReadOnly"
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
                        :disabled="isReadOnly"
                        type="date"
                        class="form-control"
                        name="last_data_curation_date"
                        min="1990-01-01"
                        :max="new Date().toISOString().split('T')[0]"
                        @blur="checkDate()"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label
                    for="conservation_plan_exists"
                    class="col-sm-3 col-form-label"
                    >Conservation Plan Exists:
                </label>
                <div class="col-sm-9">
                    <label for="conservation_plan_exists" class="me-2"
                        >No</label
                    >
                    <input
                        id="conservation_plan_exists"
                        v-model="species_community.conservation_plan_exists"
                        :disabled="isReadOnly"
                        type="radio"
                        :value="false"
                        class="form-check-input me-2"
                    />
                    <label for="conservation_plan_exists" class="me-2"
                        >Yes</label
                    >
                    <input
                        id="conservation_plan_exists"
                        v-model="species_community.conservation_plan_exists"
                        :disabled="isReadOnly"
                        type="radio"
                        :value="true"
                        class="form-check-input"
                        @change="focusConservationPlanReference"
                    />
                </div>
            </div>
            <div
                v-if="species_community.conservation_plan_exists"
                class="row mb-3"
            >
                <label
                    for="conservation_plan_reference"
                    class="col-sm-3 col-form-label"
                    >Conservation Plan Reference:
                </label>
                <div class="col-sm-9">
                    <input
                        ref="conservation_plan_reference"
                        v-model="species_community.conservation_plan_reference"
                        :disabled="isReadOnly"
                        type="text"
                        class="form-control"
                        name="conservation_plan_reference"
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label"
                    >{{ species_original.species_number }} Comment:</label
                >
                <div class="col-sm-8">
                    <textarea
                        id="comment"
                        v-model="species_original.comment"
                        :disabled="true"
                        class="form-control"
                        rows="3"
                        placeholder=""
                    />
                </div>
                <div class="col-sm-1">
                    <input
                        :id="'comment_chk' + species_community.id"
                        class="form-check-input"
                        type="checkbox"
                        @change="
                            checkCommentInput(
                                'comment_chk' + species_community.id,
                                'comment'
                            )
                        "
                    />
                </div>
            </div>
            <div class="row mb-3">
                <label for="" class="col-sm-3 control-label">Comment:</label>
                <div class="col-sm-8">
                    <textarea
                        id="comment"
                        v-model="species_community.comment"
                        :disabled="isReadOnly"
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
import { v4 as uuid } from 'uuid';
import FormSection from '@/components/forms/section_toggle.vue';
import { api_endpoints, helpers } from '@/utils/hooks';
export default {
    name: 'SpeciesSplitProfile',
    components: {
        FormSection,
    },
    props: {
        species_community: {
            type: Object,
            required: true,
        },
        species_original: {
            type: Object,
            required: true,
        },
        splitSpeciesListContainsOriginalTaxonomy: {
            type: Boolean,
            required: true,
        },
        selectedTaxonomies: {
            type: Array,
            required: true,
        },
    },
    data: function () {
        let vm = this;
        return {
            scientific_name_lookup:
                'scientific_name_lookup' + vm.species_community.index,
            select_scientific_name:
                'select_scientific_name' + vm.species_community.index,
            select_flowering_period:
                'select_flowering_period' + vm.species_community.index,
            select_flowering_period_readonly:
                'select_flowering_period_readonly' + vm.species_community.index,
            select_fruiting_period:
                'select_fruiting_period' + vm.species_community.index,
            select_fruiting_period_readonly:
                'select_fruiting_period_readonly' + vm.species_community.index,
            select_breeding_period:
                'select_breeding_period' + vm.species_community.index,
            select_breeding_period_readonly:
                'select_breeding_period_readonly' + vm.species_community.index,
            select_regions: 'select_regions' + vm.species_community.index,
            select_regions_read_only:
                'select_regions_read_only' + vm.species_community.index,
            select_districts_read_only:
                'select_districts_read_only' + vm.species_community.index,
            select_districts: 'select_districts' + vm.species_community.index,
            taxonBody: 'taxonBody' + uuid(),
            distributionBody: 'distributionBody' + uuid(),
            conservationBody: 'conservationBody' + uuid(),
            generalBody: 'generalBody' + uuid(),
            retainOriginalSpecies: false,
            //---to show fields related to Fauna
            isFauna: vm.species_community.group_type === 'fauna' ? true : false,
            //----list of values dictionary
            species_profile_dict: {},
            //scientific_name_list: [],
            region_list: [],
            district_list: [],
            district_list_readonly: [],
            filtered_district_list: [],
            //---conservatiuon attributes field lists
            flora_recruitment_type_list: [],
            root_morphology_list: [],
            post_fire_habitatat_interactions_list: [],
            // to display the species Taxonomy selected details
            species_display: '',
            common_name: null,
            taxon_name_id: null,
            taxon_previous_name: null,
            phylogenetic_group: null,
            family: null,
            genus: null,
            name_authority: null,
            name_comments: null,
            species_id: null,
            period_list: [
                { id: 1, name: 'January' },
                { id: 2, name: 'February' },
                { id: 3, name: 'March' },
                { id: 4, name: 'April' },
                { id: 5, name: 'May' },
                { id: 6, name: 'June' },
                { id: 7, name: 'July' },
                { id: 8, name: 'August' },
                { id: 9, name: 'September' },
                { id: 10, name: 'October' },
                { id: 11, name: 'November' },
                { id: 12, name: 'December' },
            ],
            minimum_fire_interval_range_original: false,
            average_lifespan_range_original: false,
            generation_length_range_original: false,
            time_to_maturity_range_original: false,
            minimum_fire_interval_range_new: false,
            average_lifespan_range_new: false,
            generation_length_range_new: false,
            time_to_maturity_range_new: false,
            interval_choice: [
                { id: 1, name: 'year/s' },
                { id: 2, name: 'month/s' },
            ],
            errors: {
                minimum_fire_interval_error: null,
                average_lifespan_error: null,
                generation_length_error: null,
                time_to_maturity_error: null,
            },
        };
    },
    computed: {
        isReadOnly: function () {
            let action = this.$route.query.action;
            if (
                action === 'edit' &&
                this.species_community &&
                this.species_community.user_edit_mode
            ) {
                return false;
            } else {
                return this.species_community.readonly;
            }
        },
        showRetainOriginalSpeciesCheckbox: function () {
            return (
                !this.splitSpeciesListContainsOriginalTaxonomy ||
                this.species_community.taxonomy_id ===
                    this.species_original.taxonomy_id
            );
        },
        thisSplitSpeciesHasOriginalTaxonomyId: function () {
            return (
                this.species_community.taxonomy_id ===
                this.species_original.taxonomy_id
            );
        },
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
        species_id: function (newVal) {
            if (newVal) {
                this.fetchSpeciesData();
            } else {
                this.clearSpeciesData();
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
        if (
            vm.species_original.conservation_attributes
                .minimum_fire_interval_to != null &&
            vm.species_original.conservation_attributes
                .minimum_fire_interval_to != '' &&
            vm.species_original.conservation_attributes
                .minimum_fire_interval_to != undefined
        ) {
            vm.minimum_fire_interval_range_original = true;
        }
        if (
            vm.species_community.conservation_attributes
                .minimum_fire_interval_to != null &&
            vm.species_community.conservation_attributes
                .minimum_fire_interval_to != '' &&
            vm.species_community.conservation_attributes
                .minimum_fire_interval_to != undefined
        ) {
            vm.minimum_fire_interval_range_new = true;
        }
        if (
            vm.species_original.conservation_attributes.average_lifespan_to !=
                null &&
            vm.species_original.conservation_attributes.average_lifespan_to !=
                '' &&
            vm.species_original.conservation_attributes.average_lifespan_to !=
                undefined
        ) {
            vm.average_lifespan_range_original = true;
        }
        if (
            vm.species_community.conservation_attributes.average_lifespan_to !=
                null &&
            vm.species_community.conservation_attributes.average_lifespan_to !=
                '' &&
            vm.species_community.conservation_attributes.average_lifespan_to !=
                undefined
        ) {
            vm.average_lifespan_range_new = true;
        }
        if (
            vm.species_original.conservation_attributes.generation_length_to !=
                null &&
            vm.species_original.conservation_attributes.generation_length_to !=
                '' &&
            vm.species_original.conservation_attributes.generation_length_to !=
                undefined
        ) {
            vm.generation_length_range_original = true;
        }
        if (
            vm.species_community.conservation_attributes.generation_length_to !=
                null &&
            vm.species_community.conservation_attributes.generation_length_to !=
                '' &&
            vm.species_community.conservation_attributes.generation_length_to !=
                undefined
        ) {
            vm.generation_length_range_new = true;
        }
        if (
            vm.species_original.conservation_attributes.time_to_maturity_to !=
                null &&
            vm.species_original.conservation_attributes.time_to_maturity_to !=
                '' &&
            vm.species_original.conservation_attributes.time_to_maturity_to !=
                undefined
        ) {
            vm.time_to_maturity_range_original = true;
        }
        if (
            vm.species_community.conservation_attributes.time_to_maturity_to !=
                null &&
            vm.species_community.conservation_attributes.time_to_maturity_to !=
                '' &&
            vm.species_community.conservation_attributes.time_to_maturity_to !=
                undefined
        ) {
            vm.time_to_maturity_range_new = true;
        }
        //------fetch list of values
        const response = await fetch('/api/species_profile_dict/');
        vm.species_profile_dict = await response.json();
        vm.flora_recruitment_type_list =
            vm.species_profile_dict.flora_recruitment_type_list;
        vm.flora_recruitment_type_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.root_morphology_list = vm.species_profile_dict.root_morphology_list;
        vm.root_morphology_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.post_fire_habitatat_interactions_list =
            vm.species_profile_dict.post_fire_habitatat_interactions_list;
        vm.post_fire_habitatat_interactions_list.splice(0, 0, {
            id: null,
            name: null,
        });
        vm.fetchRegions();
    },
    mounted: function () {
        let vm = this;
        vm.eventListeners();
        vm.initialiseScientificNameLookup();
        vm.loadTaxonomydetails();
        vm.initialiseRegionSelect();
        vm.initialiseDistrictSelect();
    },
    methods: {
        filterDistrict: function (event) {
            this.$nextTick(() => {
                if (event) {
                    this.species_community.district_id = null; //-----to remove the previous selection
                }
                this.filtered_district_list = [];
                this.filtered_district_list = [
                    {
                        id: null,
                        name: '',
                        region_id: null,
                    },
                ];
                //---filter districts as per region selected
                for (let choice of this.district_list) {
                    if (choice.region_id === this.species_community.region_id) {
                        this.filtered_district_list.push(choice);
                    }
                }
            });
        },
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
        resetTaxonomyDetails: function () {
            let vm = this;
            if (vm.species_community.taxonomy_details) {
                vm.species_community.taxonomy_details = null;
            }
            vm.species_community.taxonomy_id = '';
            vm.species_display = '';
            vm.common_name = '';
            vm.taxon_name_id = '';
            vm.taxon_previous_name = '';
            vm.phylogenetic_group = '';
            vm.family = '';
            vm.genus = '';
            vm.name_authority = '';
            vm.name_comments = '';
            vm.species_id = null;
        },
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
                                group_type_id:
                                    vm.species_community.group_type_id,
                                species_profile: false,
                                exclude_taxonomy_ids: vm.selectedTaxonomies,
                            };
                            return query;
                        },
                    },
                })
                .on('select2:selecting', function (e) {
                    let data = e.params.args.data.id;
                    if (
                        vm.splitSpeciesListContainsOriginalTaxonomy &&
                        vm.species_original.taxonomy_id === data
                    ) {
                        e.preventDefault();
                        swal.fire({
                            title: 'Original Taxonomy Already Selected',
                            text: 'There is already a split species with the same taxonomy as the original species.',
                            icon: 'info',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                            didClose: () => {
                                vm.resetTaxonomyDetails();
                                vm.$nextTick(() => {
                                    $(
                                        vm.$refs[vm.scientific_name_lookup]
                                    ).select2('open');
                                });
                            },
                        });
                        return false;
                    }
                    if (vm.selectedTaxonomies.includes(data)) {
                        e.preventDefault();
                        swal.fire({
                            title: 'Taxonomy Already Selected',
                            text: 'This taxonomy is already selected in another split species.',
                            icon: 'info',
                            customClass: {
                                confirmButton: 'btn btn-primary',
                            },
                            didClose: () => {
                                vm.resetTaxonomyDetails();
                                vm.$nextTick(() => {
                                    $(
                                        vm.$refs[vm.scientific_name_lookup]
                                    ).select2('open');
                                });
                            },
                        });
                        return false;
                    }
                })
                .on('select2:select', function (e) {
                    let data = e.params.data.id;
                    if (
                        vm.retainOriginalSpecies &&
                        data != vm.species_original.taxonomy_id
                    ) {
                        vm.retainOriginalSpecies = false;
                    }
                    if (
                        !vm.retainOriginalSpecies &&
                        data == vm.species_original.taxonomy_id
                    ) {
                        vm.retainOriginalSpecies = true;
                    }
                    vm.species_community.taxonomy_id = data;
                    vm.species_community.taxonomy_details = e.params.data;
                    vm.species_display = e.params.data.scientific_name;
                    vm.common_name = e.params.data.common_name;
                    vm.taxon_name_id = e.params.data.taxon_name_id;
                    vm.taxon_previous_name = e.params.data.taxon_previous_name;
                    vm.phylogenetic_group = e.params.data.phylogenetic_group;
                    vm.family = e.params.data.family_name;
                    vm.genus = e.params.data.genera_name;
                    vm.name_authority = e.params.data.name_authority;
                    vm.name_comments = e.params.data.name_comments;
                    vm.species_id = e.params.data.species_id;
                })
                .on('select2:unselect', function () {
                    vm.resetTaxonomyDetails();
                    vm.species_community.taxonomy_id = '';
                    vm.species_community.taxonomy_details = null;
                    vm.species_id = null;
                    vm.retainOriginalSpecies = false;
                })
                .on('select2:clear', function () {
                    vm.resetTaxonomyDetails();
                    vm.species_community.taxonomy_id = '';
                    vm.species_community.taxonomy_details = null;
                    vm.species_id = null;
                    vm.retainOriginalSpecies = false;
                })
                .on('select2:open', function () {
                    const searchField = $(
                        '[aria-controls="select2-' +
                            vm.scientific_name_lookup +
                            '-results"]'
                    );
                    // move focus to select2 field
                    searchField[0].focus();
                });
        },
        loadTaxonomydetails: function () {
            let vm = this;
            if (vm.species_community.taxonomy_details != null) {
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
        //--------on/off checkbox value to new species--------
        checkHabitatForm: function () {
            if (
                $('#habitat_frm_chk' + this.species_community.id).is(
                    ':checked'
                ) == true
            ) {
                this.species_community.conservation_attributes.habitat_growth_form =
                    this.species_original.conservation_attributes.habitat_growth_form;
            } else {
                this.species_community.conservation_attributes.habitat_growth_form =
                    null;
            }
        },
        checkConservationInput: function (chkbox, obj_field, select2_ref = '') {
            const interval_fields = [
                'minimum_fire_interval',
                'average_lifespan',
                'generation_length',
                'time_to_maturity',
            ];
            // if checkbox is checked copy value from original  species to new species
            if ($('#' + chkbox).is(':checked') == true) {
                if (interval_fields.includes(obj_field)) {
                    this.species_community.conservation_attributes[
                        obj_field + '_from'
                    ] =
                        this.species_original.conservation_attributes[
                            obj_field + '_from'
                        ];
                    this.species_community.conservation_attributes[
                        obj_field + '_to'
                    ] =
                        this.species_original.conservation_attributes[
                            obj_field + '_to'
                        ];
                    this.species_community.conservation_attributes[
                        obj_field + '_choice'
                    ] =
                        this.species_original.conservation_attributes[
                            obj_field + '_choice'
                        ];
                    if (
                        this.species_community.conservation_attributes[
                            obj_field + '_to'
                        ] != null
                    ) {
                        this[obj_field + '_range_new'] = true;
                    } else {
                        this[obj_field + '_range_new'] = false;
                    }
                } else {
                    this.species_community.conservation_attributes[obj_field] =
                        this.species_original.conservation_attributes[
                            obj_field
                        ];
                    if (select2_ref != '') {
                        $(this.$refs[select2_ref])
                            .val(
                                this.species_community.conservation_attributes[
                                    obj_field
                                ]
                            )
                            .trigger('change');
                    }
                }
            } else {
                if (interval_fields.includes(obj_field)) {
                    this.species_community.conservation_attributes[
                        obj_field + '_from'
                    ] = null;
                    this.species_community.conservation_attributes[
                        obj_field + '_to'
                    ] = null;
                    this.species_community.conservation_attributes[
                        obj_field + '_choice'
                    ] = null;
                    this[obj_field + '_range_new'] = false;
                } else {
                    this.species_community.conservation_attributes[obj_field] =
                        null;
                    if (select2_ref != '') {
                        $(this.$refs[select2_ref]).val('').trigger('change');
                        this.species_community.conservation_attributes[
                            obj_field
                        ] = [];
                    }
                }
            }
        },
        checkDistributionInput: function (chkbox, obj_field) {
            // if checkbox is checked copy value from original  species to new species
            if ($('#' + chkbox).is(':checked') == true) {
                this.species_community.distribution[obj_field] =
                    this.species_original.distribution[obj_field];
            } else {
                this.species_community.distribution[obj_field] = null;
            }
        },
        checkCommentInput: function (chkbox, obj_field) {
            // if checkbox is checked copy value from original  species to new species
            if ($('#' + chkbox).is(':checked') == true) {
                this.species_community[obj_field] =
                    this.species_original[obj_field];
            } else {
                this.species_community[obj_field] = null;
            }
        },
        checkRegionDistrictInput: function (
            chkbox,
            obj_field,
            select2_ref = ''
        ) {
            let vm = this;
            // if checkbox is checked copy value from original  species to new species
            if ($('#' + chkbox).is(':checked') == true) {
                this.species_community[obj_field] =
                    this.species_original[obj_field];
                if (select2_ref != '') {
                    $(this.$refs[select2_ref])
                        .val(this.species_community[obj_field])
                        .trigger('change');
                    this.chainedSelectDistricts(
                        this.species_community.regions,
                        'select'
                    );
                }
            } else {
                if (select2_ref != '') {
                    $(this.$refs[select2_ref])
                        .val(vm.species_community.regions || '')
                        .trigger('change');
                    this.chainedSelectDistricts(
                        this.species_community.regions,
                        'deselect'
                    );
                }
                this.species_community[obj_field] = [];
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
        //----------------------------------------------------------------
        eventListeners: function () {
            let vm = this;
            $(vm.$refs.flowering_period_select)
                .select2({
                    dropdownParent: $('#' + vm.select_flowering_period),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Flowering Period',
                    multiple: true,
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.conservation_attributes.flowering_period =
                        selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.conservation_attributes.flowering_period =
                        selected.val();
                });
            $(vm.$refs.flowering_period_select_readonly).select2({
                dropdownParent: $('#' + vm.select_flowering_period_readonly),
                theme: 'bootstrap-5',
                allowClear: true,
                placeholder: 'Select Flowering Period',
                multiple: true,
            });
            $(vm.$refs.fruiting_period_select)
                .select2({
                    dropdownParent: $('#' + vm.select_fruiting_period),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Fruiting Period',
                    multiple: true,
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.conservation_attributes.fruiting_period =
                        selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.conservation_attributes.fruiting_period =
                        selected.val();
                });
            $(vm.$refs.fruiting_period_select_readonly).select2({
                dropdownParent: $('#' + vm.select_fruiting_period_readonly),
                theme: 'bootstrap-5',
                allowClear: true,
                placeholder: 'Select Fruiting Period',
                multiple: true,
            });
            $(vm.$refs.breeding_period_select)
                .select2({
                    dropdownParent: $('#' + vm.select_breeding_period),
                    theme: 'bootstrap-5',
                    allowClear: true,
                    placeholder: 'Select Breeding Period',
                    multiple: true,
                })
                .on('select2:select', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.conservation_attributes.breeding_period =
                        selected.val();
                })
                .on('select2:unselect', function (e) {
                    var selected = $(e.currentTarget);
                    vm.species_community.conservation_attributes.breeding_period =
                        selected.val();
                });
            $(vm.$refs.breeding_period_select_readonly).select2({
                dropdownParent: $('#' + vm.select_breeding_period_readonly),
                theme: 'bootstrap-5',
                allowClear: true,
                placeholder: 'Select Breeding Period',
                multiple: true,
            });
        },
        intervalMonthsComputedNew: function (field_from, field_choice) {
            const totalMonths = parseInt(
                this.species_community.conservation_attributes[field_from]
            );
            const intervalChoice =
                this.species_community.conservation_attributes[field_choice];
            // const isIntervalRange = this.minimum_fire_interval_range_new;

            // if(totalMonths > 12 && intervalChoice == 2 && isIntervalRange == false){
            if (totalMonths > 12 && intervalChoice == 2) {
                const years = Math.floor(totalMonths / 12);
                const months = totalMonths % 12;
                return years + ' year/s ' + months + ' month/s';
            } else {
                return '';
            }
        },
        intervalMonthsComputedOriginal: function (months, intervalChoice) {
            const totalMonths = parseInt(months);

            if (totalMonths > 12 && intervalChoice == 2) {
                const years = Math.floor(totalMonths / 12);
                const months = totalMonths % 12;
                return years + ' year/s ' + months + ' month/s';
            } else {
                return '';
            }
        },
        handleMinimumFireIntervalRange: function () {
            if (this.minimum_fire_interval_range_new == false) {
                this.species_community.conservation_attributes.minimum_fire_interval_to =
                    null;
            }
        },
        handleAverageLifespanRange: function () {
            if (this.average_lifespan_range_new == false) {
                this.species_community.conservation_attributes.average_lifespan_to =
                    null;
            }
        },
        handleGenerationLengthRange: function () {
            if (this.generation_length_range_new == false) {
                this.species_community.conservation_attributes.generation_length_to =
                    null;
            }
        },
        handleTimeToMaturityRange: function () {
            if (this.time_to_maturity_range_new == false) {
                this.species_community.conservation_attributes.time_to_maturity_to =
                    null;
            }
        },
        validateRange: function (
            field_from,
            field_to,
            field_choice,
            field_error
        ) {
            const rangeFrom = parseInt(
                this.species_community.conservation_attributes[field_from]
            );
            const rangeTo = parseInt(
                this.species_community.conservation_attributes[field_to]
            );
            const intervalChoice =
                this.species_community.conservation_attributes[field_choice];
            if (
                (rangeFrom != null || rangeTo != null) &&
                intervalChoice == null
            ) {
                this.errors[field_error] = 'Please select years/months';
            } else if (rangeFrom >= rangeTo) {
                this.errors[field_error] = 'Please enter a valid range';
            } else {
                this.errors[field_error] = '';
            }
        },
        focusConservationPlanReference: function () {
            this.$nextTick(() => {
                this.$refs.conservation_plan_reference.focus();
            });
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
                    if (vm.species_community.regions) {
                        vm.chainedSelectDistricts(
                            vm.species_community.regions,
                            'fetch'
                        );
                    }
                    if (vm.species_original.regions) {
                        vm.chainedSelectReadonlyDistricts(
                            vm.species_original.regions,
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
            /* Searches for dictionary in list */
            for (var i = 0; i < search_list.length; i++) {
                if (search_list[i].value == id) {
                    return search_list[i];
                }
            }
            return [];
        },
        chainedSelectDistricts: function (regions, action) {
            let vm = this;
            if (action != 'fetch' && action != 'select') {
                $(vm.$refs.districts_select)
                    .val(vm.species_community.districts || '')
                    .trigger('change');
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
        chainedSelectReadonlyDistricts: function (regions) {
            let vm = this;

            vm.district_list_readonly = [];
            if (regions) {
                for (let r of regions) {
                    var api_districts = this.searchList(
                        r,
                        vm.region_list
                    ).districts;
                    if (api_districts.length > 0) {
                        for (var i = 0; i < api_districts.length; i++) {
                            this.district_list_readonly.push({
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
            $(vm.$refs.regions_select_read_only).select2({
                theme: 'bootstrap-5',
                allowClear: true,
                multiple: true,
                dropdownParent: $('#' + vm.select_regions_read_only),
                placeholder: 'Select Region',
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
            $(vm.$refs.districts_select_read_only).select2({
                theme: 'bootstrap-5',
                allowClear: true,
                dropdownParent: $('#' + vm.select_districts_read_only),
                multiple: true,
                placeholder: 'Select District',
            });
        },
        toggleRetainOriginalSpecies: function (event) {
            this.retainOriginalSpecies = event.target.checked;
            if (this.retainOriginalSpecies) {
                // 1. Add the option if it doesn't exist
                if (
                    $(this.$refs[this.scientific_name_lookup]).find(
                        "option[value='" +
                            this.species_original.taxonomy_details.id +
                            "']"
                    ).length === 0
                ) {
                    $(this.$refs[this.scientific_name_lookup]).append(
                        new Option(
                            this.species_original.taxonomy_details.text,
                            this.species_original.taxonomy_details.id,
                            true,
                            true
                        )
                    );
                }

                // Manually call the logic from select2:select
                const data = this.species_original.taxonomy_details;
                this.species_community.taxonomy_details = data;
                this.species_display = data.scientific_name;
                this.common_name = data.common_name;
                this.taxon_name_id = data.taxon_name_id;
                this.taxon_previous_name = data.taxon_previous_name;
                this.phylogenetic_group = data.phylogenetic_group;
                this.family = data.family_name;
                this.genus = data.genera_name;
                this.name_authority = data.name_authority;
                this.name_comments = data.name_comments;

                this.species_community.taxonomy_id =
                    this.species_original.taxonomy_details.id;
                $(this.$refs[this.scientific_name_lookup])
                    .val(this.species_original.taxonomy_details.id)
                    .trigger('change');
            } else {
                $(this.$refs[this.scientific_name_lookup])
                    .val('')
                    .trigger('change');
                $(this.$refs[this.scientific_name_lookup]).select2('open');
                this.resetTaxonomyDetails();
            }
        },
        fetchSpeciesData: function () {
            fetch(
                helpers.add_endpoint_json(
                    api_endpoints.species,
                    this.species_id
                )
            )
                .then((response) => response.json())
                .then((data) => {
                    // Set the data for the general and distribution sections and regions/districts
                    this.species_community.department_file_numbers =
                        data.department_file_numbers;
                    this.species_community.last_data_curation_date =
                        data.last_data_curation_date;
                    this.species_community.conservation_plan_exists =
                        data.conservation_plan_exists;
                    this.species_community.conservation_plan_reference =
                        data.conservation_plan_reference;
                    this.species_community.comment = data.comment;
                    this.species_community.distribution = data.distribution;
                    // For each region in the data, populate the regions select2
                    this.species_community.regions = data.regions;
                    this.$refs.regions_select
                        .select2('val', data.regions)
                        .trigger('change');
                    this.species_community.districts = data.districts;
                    this.$refs.districts_select
                        .select2('val', data.districts)
                        .trigger('change');
                })
                .catch((error) => {
                    console.error('Error fetching species data:', error);
                });
        },
        resetSpeciesData: function () {
            this.species_community.department_file_numbers =
                this.species_original.department_file_numbers;
            this.species_community.last_data_curation_date =
                this.species_original.last_data_curation_date;
            this.species_community.conservation_plan_exists =
                this.species_original.conservation_plan_exists;
            this.species_community.conservation_plan_reference =
                this.species_original.conservation_plan_reference;
            this.species_community.comment = this.species_original.comment;
            this.species_community.distribution =
                this.species_original.distribution;
            this.species_community.regions = this.species_original.regions;
            this.species_community.districts = this.species_original.districts;
        },
    },
};
</script>

<style lang="css" scoped>
fieldset.scheduler-border {
    border: 1px groove #ddd !important;
    padding: 0 1.4em 1.4em 1.4em !important;
    margin: 0 0 1.5em 0 !important;
    -webkit-box-shadow: 0px 0px 0px 0px #000;
    box-shadow: 0px 0px 0px 0px #000;
}

legend.scheduler-border {
    width: inherit;
    /* Or auto */
    padding: 0 10px;
    /* To give a bit of padding on the left and right */
    border-bottom: none;
}

input[type='text'],
select {
    width: 100%;
}

input[type='number'] {
    width: 50%;
}

.interval-margin {
    margin-right: -100px;
}

.interval-range-true-input {
    width: 20%;
    margin-left: -80px;
}

.interval-range-true-choice {
    width: 20%;
}
</style>
