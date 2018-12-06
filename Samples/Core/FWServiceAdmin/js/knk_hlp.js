
/*format dates, uses moment.js */
ko.bindingHandlers.dateString = {
    update: function (element, valueAccessor, allBindingsAccessor, viewModel) {
        var allBindings = allBindingsAccessor();
        var value = valueAccessor();
        var valueUnwrapped = ko.utils.unwrapObservable(value);

        var pattern = allBindings.datePattern || 'mmmm d, yyyy';
        if (valueUnwrapped == undefined || valueUnwrapped == null) {
            $(element).text("");
        }
        else if (!$.isNumeric(valueUnwrapped)) {
            var date = moment(valueUnwrapped, "YYYY-MM-DDTHH:mm:ss");
            if (date.isValid())
                $(element).text(moment(date).format(pattern));
            else
                $(element).text(valueUnwrapped);
        }
        else
            $(element).text(valueUnwrapped);
        }
}