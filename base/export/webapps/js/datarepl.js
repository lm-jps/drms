<!--Download the complete list of published series-->
var gCodeTree = '/home/jsoc/cvs/Development/JSOC'
// var gCgiBinBaseUrl = '/Volumes/arta/jsoctrees/JSOC/base/drms/replication/publishseries';
// var gCgiBinBaseUrl = '../../cgi-bin/ajax';
var gCgiBinBaseUrl = 'http://jsoc2.stanford.edu/cgi-bin/ajax';


function listPopulator(listObj, list)
{
    for (var series in list)
    {
        listObj.append('<li class="ui-widget-content ui-state-default">' + list[series] + '</li>');
    }
};

<!--Called when the DOM has completed loading.-->
$(document).ready(function()
{
    var pubListUrl = gCgiBinBaseUrl + '/publist.py';
    var cfgFile = gCodeTree + '/proj/replication/etc/repserver.cfg';
                  
    alert("the eagle has landed");
                  
    // Everything having to do with the publist select control.
    $(function()
    {
        $("#publist").selectable();

        // Fetch the list of published series from the PostgreSQL database.
        $.ajax(
        {
            url: pubListUrl,
            data: {"c": cfgFile},
            success: function(data, textStatus, xhr){ var plc = new listPopulator($("#publist"), data.publist.list); },
            error: function(xhr, textStatus, errorThrown){ alert('Something done went wrong ' + textStatus); },
            dataType: 'json'
        });
               

    });
                  
    $("#date").datepicker();


                  
    
                  
});

