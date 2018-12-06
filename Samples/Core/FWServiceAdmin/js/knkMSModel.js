/*Microservice TaskStateReport view model*/
function MCSState(gsURL,frUrl) {
    var self = this;

    //it is basic description of the model. All other properties will be added in getState function. 
    //Dynamic binding.

    self.viewModel = {
        status: ko.observable(-1)
    };

    self.getStatusURL = gsURL;
    self.setFreezeURL = frUrl;

    self.freezeButtonText = function () {
        return (self.viewModel.status() == 0) ? "Freeze" : "Unfreeze";
    }
    self.freezeButtonCss = function () {
        return (self.viewModel.status() == 0) ? "btn-primary" : "btn-warning";
    }

    //get state of a service
    self.getState = function () {
        $.getJSON(self.getStatusURL, function (data) {
            if (self.viewModel.status() < 0) {               // different approach $.isEmptyObject(self.viewModel)
                self.viewModel = ko.mapping.fromJS(data);
                ko.applyBindings(self);
            }
            ko.mapping.fromJS(data, self.viewModel);
        });

    }

    //freeze a service
    self.freeze = function () {
        $.ajax({
            url: self.setFreezeURL,
            data: ko.toJSON({ freeze: (self.viewModel.status() == 0) }),
            type: "post",
            contentType: "application/json",
            dataType: "json",
            success: function (data) {
                ko.mapping.fromJS(data, self.viewModel);
            },
            error: function (xhr, status, error) {
                $("#dvState").html("Result: " + status + " " + error + " " + xhr.status + " " + xhr.statusText)
            }
        });
    }

}
