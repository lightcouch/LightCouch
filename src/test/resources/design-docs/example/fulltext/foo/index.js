function (doc) {

    if (doc.title == undefined)
        return;

    function summary(obj) {
        var s = ""
        for (var key in obj) {
            if (key.charAt(0) == "_" || key == "clazz")
                continue;
            switch (typeof obj[key]) {
                case 'object':
                    break;
                case 'function':
                    break;
                default:
                    s += obj[key] + " ";
                    break;
            }
        }
        return s;
    }

    var ret = new Document();
    ret.add(doc.title);
    ret.add(summary(doc), { "field" : "summary", "store" : "yes" })

    if (doc._attachments) {
        for (var i in doc._attachments)
            ret.attachment("attachments", i);
    }

    return ret;
}
