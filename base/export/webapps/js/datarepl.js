// Download the complete list of published series
var gCgiBinBaseUrl = '../../cgi-bin/ajax';


function seriesDict(listObj, list)
{
    this.dict = new Array();
    this.getDesc = function(series){ return this.dict[series]; };
    this.getSelected = function(){ return this.selected; };
    this.setSelected = function(series)
                       {
                            if (this.getDesc(series) != undefined)
                            {
                                this.selected = series;
                            }
                       };

    for (var series in list)
    {
        // Create a list item for each series.
        listObj.append('<li class="sellist-item ui-widget-content ui-state-default">' + list[series][0] + '</li>');

        // Map series name to description.
        this.dict[list[series][0]] = list[series][1];
    }
};

// Called when the DOM has completed loading.
$(document).ready(function()
{
    var pubListUrl = gCgiBinBaseUrl + '/' + gPublistFile;
    var cfgFile = gCfgFile;
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
                if (sDict != undefined)
                {
                    sDict.setSelected($(ui.selected).text());
                }
            },
                                 
            stop: function(event, ui)
            {
                if (sDict != undefined)
                {
                    var series = sDict.getSelected();
                    var desc = sDict.getDesc(series);
                                 
                    if (desc != undefined)
                    {
                        $("#series-desc").text(desc);
                    }
                }
            }
        });
        
        // Fetch the list of published series from the PostgreSQL database.
        $.ajax(
        {
            url: pubListUrl,
            data: {"c" : cfgFile, "d" : 1},
            success: function(data, textStatus, xhr){ sDict = new seriesDict($("#publist"), data.publist.list); },
            error: function(xhr, textStatus, errorThrown){ alert('Something done went wrong ' + textStatus); },
            dataType: 'json'
        });
               

    });
                  
    // Everything having to do with the accordion.
    $(function()
    {
        $("#accordion").accordion(
        {
            heightStyle: "fill"
        });
      
        $("#accordion-resizer").resizable(
        {
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

