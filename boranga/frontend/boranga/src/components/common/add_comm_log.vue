<template lang="html">
    <div id="AddComms">
        <modal
            transition="modal fade"
            title="Communication Log - Add Entry"
            large
            @ok="ok()"
            @cancel="cancel()"
        >
            <div class="container-fluid">
                <div class="row">
                    <form class="form-horizontal" name="commsForm">
                        <alert v-if="errorString" type="danger"
                            ><strong>{{ errorString }}</strong></alert
                        >
                        <div class="col-sm-12">
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >To</label
                                        >
                                    </div>
                                    <div class="col-sm-4">
                                        <input
                                            ref="to"
                                            v-model="comms.to"
                                            type="text"
                                            class="form-control"
                                            name="to"
                                        />
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >From</label
                                        >
                                    </div>
                                    <div class="col-sm-4">
                                        <input
                                            v-model="comms.fromm"
                                            type="text"
                                            class="form-control"
                                            name="fromm"
                                        />
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >Type</label
                                        >
                                    </div>
                                    <div class="col-sm-4">
                                        <select
                                            v-model="comms.type"
                                            class="form-select"
                                            name="type"
                                        >
                                            <option value="">
                                                Select Type
                                            </option>
                                            <option value="email">Email</option>
                                            <option value="mail">Mail</option>
                                            <option value="phone">Phone</option>
                                        </select>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >Subject/Description</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <input
                                            v-model="comms.subject"
                                            type="text"
                                            class="form-control"
                                            name="subject"
                                            style="width: 70%"
                                        />
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >Text</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <textarea
                                            v-model="comms.text"
                                            name="text"
                                            class="form-control"
                                            style="width: 70%"
                                        ></textarea>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                <div class="row mb-3">
                                    <div class="col-sm-3">
                                        <label
                                            class="control-label pull-left"
                                            for="Name"
                                            >Attachments</label
                                        >
                                    </div>
                                    <div class="col-sm-9">
                                        <template
                                            v-for="(f, i) in files"
                                            :key="i"
                                        >
                                            <div
                                                :class="
                                                    'row top-buffer file-row-' +
                                                    i
                                                "
                                            >
                                                <div class="col-sm-3">
                                                    <span
                                                        v-if="f.file == null"
                                                        class="btn btn-primary btn-file pull-left"
                                                    >
                                                        Attach File
                                                        <input
                                                            type="file"
                                                            :name="
                                                                'file-upload-' +
                                                                i
                                                            "
                                                            :class="
                                                                'file-upload-' +
                                                                i
                                                            "
                                                            @change="
                                                                uploadFile(
                                                                    'file-upload-' +
                                                                        i,
                                                                    f
                                                                )
                                                            "
                                                        />
                                                    </span>
                                                    <span
                                                        v-else
                                                        class="btn btn-primary btn-file pull-left"
                                                    >
                                                        Update File
                                                        <input
                                                            type="file"
                                                            :name="
                                                                'file-upload-' +
                                                                i
                                                            "
                                                            :class="
                                                                'file-upload-' +
                                                                i
                                                            "
                                                            @change="
                                                                uploadFile(
                                                                    'file-upload-' +
                                                                        i,
                                                                    f
                                                                )
                                                            "
                                                        />
                                                    </span>
                                                </div>
                                                <div
                                                    class="col-sm-6 truncate-text"
                                                >
                                                    <span>{{ f.name }}</span>
                                                </div>
                                                <div class="col-sm-3">
                                                    <a
                                                        href=""
                                                        style="color: red"
                                                        @click.prevent="
                                                            removeFile(i)
                                                        "
                                                        >Remove</a
                                                    >
                                                </div>
                                            </div>
                                        </template>
                                        <a
                                            href=""
                                            @click.prevent="attachAnother"
                                            ><i
                                                class="fa fa-lg fa-plus top-buffer-2x"
                                            ></i
                                        ></a>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </form>
                </div>
            </div>
        </modal>
    </div>
</template>

<script>
import modal from '@vue-utils/bootstrap-modal.vue';
import alert from '@vue-utils/alert.vue';
export default {
    name: 'AddComms',
    components: {
        modal,
        alert,
    },
    props: {
        url: {
            type: String,
            required: true,
        },
    },
    data: function () {
        return {
            isModalOpen: false,
            form: null,
            comms: {
                type: '',
            },
            state: 'proposed_approval',
            addingComms: false,
            validation_form: null,
            errorString: '',
            successString: '',
            success: false,
            datepickerOptions: {
                format: 'DD/MM/YYYY',
                showClear: true,
                useCurrent: false,
                keepInvalid: true,
                allowInputToggle: true,
            },
            files: [
                {
                    file: null,
                    name: '',
                },
            ],
        };
    },
    watch: {
        isModalOpen: function (val) {
            let vm = this;
            if (val) {
                vm.$nextTick(function () {
                    vm.$refs.to.focus();
                });
            }
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.commsForm;
    },
    methods: {
        ok: function () {
            let vm = this;
            if ($(vm.form).valid()) {
                vm.sendData();
            }
        },
        uploadFile(target, file_obj) {
            let _file = null;
            var input = $('.' + target)[0];
            if (input.files && input.files[0]) {
                var reader = new FileReader();
                reader.readAsDataURL(input.files[0]);
                reader.onload = function (e) {
                    _file = e.target.result;
                };
                _file = input.files[0];
            }
            file_obj.file = _file;
            file_obj.name = _file.name;
        },
        removeFile(index) {
            let length = this.files.length;
            $('.file-row-' + index).remove();
            this.files.splice(index, 1);
            this.$nextTick(() => {
                length == 1 ? this.attachAnother() : '';
            });
        },
        attachAnother() {
            this.files.push({
                file: null,
                name: '',
            });
        },
        cancel: function () {
            this.close();
        },
        close: function () {
            let vm = this;
            this.isModalOpen = false;
            this.comms = {};
            this.errorString = '';
            $('.has-error').removeClass('has-error');
            let file_length = vm.files.length;
            this.files = [];
            for (var i = 0; i < file_length; i++) {
                vm.$nextTick(() => {
                    $('.file-row-' + i).remove();
                });
            }
            this.attachAnother();
        },
        sendData: function () {
            let vm = this;
            vm.errorString = '';
            let comms = new FormData(vm.form);
            for (let i = 0; i < vm.files.length; i++) {
                comms.append('files', vm.files[i].file);
            }
            vm.addingComms = true;
            fetch(vm.url, {
                method: 'POST',
                body: comms,
            }).then(async (response) => {
                const data = await response.json();
                if (!response.ok) {
                    vm.addingComms = false;
                    vm.errorString = data;
                    return;
                }
                vm.addingComms = false;
                vm.close();
            });
        },
        addFormValidations: function () {
            let vm = this;
            vm.validation_form = $(vm.form).validate({
                rules: {
                    to: 'required',
                    fromm: 'required',
                    type: 'required',
                    subject: 'required',
                    text: 'required',
                },
                messages: {},
                showErrors: function (errorMap, errorList) {
                    $.each(this.validElements(), function (index, element) {
                        var $element = $(element);
                        $element
                            .attr('data-original-title', '')
                            .parents('.form-group')
                            .removeClass('has-error');
                    });
                    // destroy tooltips on valid elements
                    $('.' + this.settings.validClass).tooltip('destroy');
                    // add or update tooltips
                    for (var i = 0; i < errorList.length; i++) {
                        var error = errorList[i];
                        $(error.element)
                            .tooltip({
                                trigger: 'focus',
                            })
                            .attr('data-original-title', error.message)
                            .parents('.form-group')
                            .addClass('has-error');
                    }
                },
            });
        },
    },
};
</script>

<style lang="css">
.btn-file {
    position: relative;
    overflow: hidden;
}

.btn-file input[type='file'] {
    position: absolute;
    top: 0;
    right: 0;
    min-width: 100%;
    min-height: 100%;
    font-size: 100px;
    text-align: right;
    filter: alpha(opacity=0);
    opacity: 0;
    outline: none;
    background: white;
    cursor: inherit;
    display: block;
}

.top-buffer {
    margin-top: 5px;
}

.top-buffer-2x {
    margin-top: 10px;
}

input[type='text'],
select {
    padding: 0.375rem 2.25rem 0.375rem 0.75rem;
}

.truncate-text {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: block;
}

input[type='text'],
select {
    width: 100%;
    padding: 0.375rem 2.25rem 0.375rem 0.75rem;
}
</style>
