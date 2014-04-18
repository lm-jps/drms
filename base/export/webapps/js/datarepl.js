<!--Download the complete list of published series-->
var gCodeTree = '/home/jsoc/cvs/Development/JSOC'
// var gCgiBinBaseUrl = '/Volumes/arta/jsoctrees/JSOC/base/drms/replication/publishseries';
// var gCgiBinBaseUrl = '../../cgi-bin/ajax';
var gCgiBinBaseUrl = 'http://jsoc2.stanford.edu/cgi-bin/ajax';


function seriesDict(listObj, list)
{
    this.dict = new Array();
    this.getDesc = function(series){ return this.dict[series]; };

    for (var series in list)
    {
	// Create a list item for each series.
        listObj.append('<li class="ui-widget-content ui-state-default">' + list[series][0] + '</li>');

	// Map series name to description.
	this.dict[list[series][0]] = list[series][1];
    }
};

<!--Called when the DOM has completed loading.-->
$(document).ready(function()
{
    var pubListUrl = gCgiBinBaseUrl + '/publist.py';
    var cfgFile = gCodeTree + '/proj/replication/etc/repserver.cfg';
    var sDict;
                  
    alert("the eagle has landed");
                  
    // Everything having to do with the publist select control.
    $(function()
    {
        $("#publist").selectable(
        {
            selected: function(event, ui) 
            {
		// Unselect all other items (making this a single-select selectable control
                $(ui.selected).siblings().removeClass("ui-selected");

		// Display the selected series' description
		
            } 
        });
        
        // Fetch the list of published series from the PostgreSQL database.
        $.ajax(
        {
            url: pubListUrl,
            data: {"c": cfgFile},
            success: function(data, textStatus, xhr){ sDict = new seriesDict($("#publist"), data.publist.list); },
            error: function(xhr, textStatus, errorThrown){ alert('Something done went wrong ' + textStatus); },
            dataType: 'json'
        });
               

    });
                  
    // Everything having to do with the accordion.
    $(function()
    {
      $("#accordion").accordion({
                                    heightStyle: "fill"
                                });
      
      $("#accordion-resizer").resizable({
                                            minHeight: 140,
                                            minWidth: 200,
                                            resize: function()
                                                    {
                                                        $("#accordion").accordion( "refresh" );
                                                    }
                                        });
     
      
    });
                  
    $("#date").datepicker();


                  
    
                  
});

