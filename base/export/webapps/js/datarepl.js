// Download the complete list of published series
var gCgiBinBaseUrl = '../../cgi-bin/ajax';


function seriesDict(listObj, list)
{
    this.dict = {}; // Emtpy object
    this.applyFn = function(fn, args)
    {
        for (item in this.dict)
        {
            fn(item, args);
        }
    };
    this.getDesc = function(series)
    {
        return this.dict[series];
    };
    this.isValid = function(series)
    {
        return (dict[series] != undefined);
    };

    for (var series in list)
    {
        // Map series name to description.
        this.dict[list[series][0]] = list[series][1];
    }
};

function pubList(listObj, sDict)
{
    this.getSelected = function(){ return this.selected; };
    this.setSelected = function(series)
    {
        if (sDict.getDesc(series) != undefined)
        {
            this.selected = series;
        }
    };
    
    // Create a list item for each series.
    this.populate = function(series, args) { args[0].append('<li class="sellist-item ui-widget-content ui-state-default">' + series + '</li>'); };
    sDict.applyFn(this.populate, [listObj]);
};

function subList(listObj, list, sDict)
{
    this.dict = {};
    
    this.getSelected = function()
    {
        return this.selected;
    }; // an array of series
    
    this.addToSelected = function(series)
    {
        if (sDict.isValid(series))
        {
            this.selected[this.selected.length] = series;
        }
    };
    
    this.setSelected = function(seriesList) // seriesList is an array of series
    {
        if (seriesList.every(sDict.isValid))
        {
            this.selected = seriesList.slice(0);
        }
    };
    
    this.clearSelected = function()
    {
        this.selected = [];
    };
    
    this.applyFn = function(fn, args)
    {
        for (var series in this.dict)
        {
            fn(series, args);
        }
    };
    
    // Create a list item for each series.
    this.populate = function(series, args)
    {
        args[0].append('<li class="sellist-item ui-widget-content ui-state-default">' + series + '</li>');
    };

    // Copy the list to the internal dictionary. Some series will have no subscribers.
    for (var series in list)
    {
        this.dict[series] = list[series].slice(0);
    }
    
    this.applyFn(this.populate, [listObj]);
}

// Called when the DOM has completed loading.
$(document).ready(function()
{
    var pubListUrl = gCgiBinBaseUrl + '/' + gPublistFile;
    var cfgFile = gCfgFile;
    var sDict;
    var pList;
    var sList;
                  
    alert("the eagle has landed");
                  
    // Everything having to do with the publist select control.
    $(function()
    {
        $("#publist").selectable(
        {
            selected: function(event, ui) 
            {
                // Unselect all other items (making this a single-select selectable control)
                $(ui.selected).siblings().removeClass("ui-selected");

                if (pList != undefined)
                {
                    pList.setSelected($(ui.selected).text());
                }
            },

            // Display the selected series' description
            stop: function(event, ui)
            {
                if (pList != undefined && sDict != undefined)
                {
                    var series = pList.getSelected();
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
            data: {"cfg" : cfgFile, "d" : 1},
            success: function(data, textStatus, xhr){ sDict = new seriesDict($("#publist"), data.publist.list); pList = new pubList($("#publist"), sDict); },
            error: function(xhr, textStatus, errorThrown){ alert('Something done went wrong ' + textStatus); },
            dataType: 'json'
        });
    });
                  
    $(function()
    {
        $("#sublist").selectable(
        {
            start: function(event, ui)
            {
                //$(ui.selected).siblings().removeClass("ui-selected");
                //$(ui.selected).removeClass("ui-selected");
                //$("#sublist").children.removeClass("ui-selected");

                if (sList != undefined)
                {
                    sList.clearSelected();
                }
            },

            selected: function(event, ui)
            {
                if (sList != undefined)
                {
                    sList.addToSelected($(ui.selected).text());
                }
            },
                                 
            stop: function(event, ui)
            {
                // Display the selected series' description
                if (sList != undefined)
                {
                    // Need to populate another list
                }
            },
        });
      
        // Fetch the list of subscribed-to series from the PostgreSQL database.
        $.ajax(
        {
            url: pubListUrl,
            data: {"cfg" : cfgFile, "series" : 'all'},
            success: function(data, textStatus, xhr){ sList = new subList($("#sublist"), data.nodelist, sDict); },
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

