var model = {
/*  "sources":
  [{"name":"myDb"}
  ]
  ,"tables":
    [{"name":"Patient", "order":1, "visible":true, "Source":"myDb"
      ,"fields": 
        [{"name":"IdPatient","type":"column", "dataType":"string", "formule":"IdPatient","format":null, "visible":false, "order":0, "level":1, "table":"Patient"}
        ,{"name":"Nom","type":"column", "dataType":"string", "formule":"Nom","format":null, "visible":true, "order":1, "level":1, "table":"Patient"}
        ,{"name":"Prenom","type":"column", "dataType":"string", "formule":"Prenom","format":null, "visible":true, "order":2, "level":1, "table":"Patient"}
        ,{"name":"Date Naissance","type":"column", "dataType":"date", "formule":"BirthDate","format":null,"visible":true, "order":3, "level":1, "table":"Patient"}
        ,{"name":"Date Naissance.Année","type":"column", "dataType":"int", "formule":"GetYear(BirthDate)","format":null,"visible":true, "order":4, "level":2, "table":"Patient"}
        ,{"name":"Date Naissance.Mois","type":"column","dataType":"string","formule":"Format(BirthDate, 'yyyyMMM')","format":null,"visible":true,"order":5,"level":2,"orderby":"Format(Date, 'yyyyMM')","table":"Patient"}
        ,{"name":"Date Naissance.Jour","type":"column", "dataType":"int", "formule":"GetDay(BirthDate)","format":null,"visible":true, "order":6, "level":2,"table":"Patient"}
        ]
     }
    ,{"name":"Pays", "order":2, "visible":true, "Source":"myDb"
      ,"fields": 
        [{"name":"Code Pays","type":"column", "dataType":"string", "formule":"Code Pays","format":null, "visible":true, "order":1, "level":1, "table":"Pays"}
        ,{"name":"Pays","type":"column", "dataType":"string", "formule":"Pays","format":null, "visible":true, "order":2, "level":1, "table":"Pays"}
        ,{"name":"Region","type":"column", "dataType":"string", "formule":"Region","format":null, "visible":true, "order":3, "level":1, "table":"Pays"}
        ]
     }
    ,{"name":"Visite", "order":3, "visible":true, "Source":"myDb"
      ,"fields": 
        [{"name":"IdPatient","type":"column", "dataType":"string", "formule":"IdPatient","format":null, "visible":false, "order":0, "level":1, "table":"Visite"}
        ,{"name":"Age Patient","type":"column", "dataType":"date", "formule":"DateDiff(Date, Now(), 'yyyy')","format":null,"visible":true, "order":1, "level":1, "table":"Visite"}
        ,{"name":"Date","type":"column", "dataType":"date", "formule":"Date","format":null,"visible":true, "order":2, "level":1, "table":"Visite"}
        ,{"name":"Date.Année","type":"column", "dataType":"int", "formule":"GetYear(Date)","format":null,"visible":true, "order":3, "level":2, "table":"Visite"}
        ,{"name":"Date.Mois","type":"column","dataType":"string","formule":"Format(Date, 'yyyyMMM')","format":null,"visible":true,"order":4,"level":2,"orderby":"Format(Date, 'yyyyMM')", "table":"Visite"}
        ,{"name":"Date.Jour","type":"column", "dataType":"int", "formule":"GetDay(Date)","format":null,"visible":true, "order":4, "level":2, "table":"Visite"}
        ,{"name":"Code Pays","type":"column", "dataType":"string", "formule":"Code Pays","format":null, "visible":false, "order":5, "level":1, "table":"Visite"}
        ,{"name":"Code Postale","type":"column", "dataType":"string", "formule":"Code Postale","format":null, "visible":true, "order":6, "level":1, "table":"Visite"}
        ]
    }
  ]
  ,"relationships":[
      {"fromTable":"Visite", "fromColumn":"IdPatient", "toTable":"Patient", "toColumn":"IdPatient", "primary":true, "direction":">"}
      ,{"fromTable":"Visite", "fromColumn":"Code Pays", "toTable":"Pays", "toColumn":"Code Pays", "primary":true, "direction":">"}
    ]
*/};

var cellCount=100;
var cells = [];
for(var i=0;i<cellCount;i++) {cells.push(i); }
var visuals = [];

const visualGallery = [
  {"name":"Bars", "icon":"barChart.png", "alt":"Bar Chart"
    ,"fields":[
      {"name":"Title", "type":"string", "arity":"1", "group":"data"}
      ,{"name":"Category", "type":"axis", "arity":"*", "group":"data"}
      ,{"name":"Legend", "type":"axis", "arity":"1", "group":"data"}
      ,{"name":"Measure", "type":"measure","arity":"*", "group":"data"}
    ]}
  ,{"name":"Lines", "icon":"lineChart.png", "alt":"Line Chart"
    ,"fields":[
      {"name":"Title", "type":"string", "arity":"1", "group":"data"}
      ,{"name":"Category", "type":"axis", "arity":"*", "group":"data"}
      ,{"name":"Legend", "type":"axis","arity":"1", "group":"data"}
      ,{"name":"Measure", "type":"measure", "arity":"*", "group":"data"}
    ]}
  ,{"name":"Scatter", "icon":"scatterChart.png", "alt":"Scatter Plot"
    ,"fields":[
      {"name":"Title", "type":"string", "arity":"1", "group":"data"}
      ,{"name":"X-Axis", "type":"measure","arity":"1", "group":"data"}
      ,{"name":"Y-Axis", "type":"measure", "arity":"1", "group":"data"}
      ,{"name":"Category", "type":"axis", "arity":"1", "group":"data"}
      ,{"name":"Legend", "type":"axis", "arity":"1", "group":"data"}
    ]}
  ,{"name":"Filter", "icon":"filter.png", "alt":"Filter"
    ,"fields":[
      {"name":"Field", "type":"axis", "arity":"1", "group":"data"}
    ]}
  ,{"name":"Table", "icon":"table.png", "alt":"Table"
    ,"fields":[
      {"name":"Field", "type":"axis", "arity":"*", "group":"data"}
    ]}
]

const measureFunctions = [
  {"name":"Sum", "types":["int", "numeric"]}
  ,{"name":"Count", "types":[]}
  ,{"name":"Distinct Count", "types":[]}
  ,{"name":"Average", "types":["int", "numeric", "Date"]}
  ,{"name":"Other", "types":[], "isOther":true}
]

const transformationFunctions = [
  {"name":"Other", "types":[], "isOther":true}
] 

const actionButtons = [
  {"name":"2DataQuery", "title":"Save as Query", "icon":"dataExport.png"}
  ,{"name":"DownloadData", "title":"Download data", "icon":"dataDownload.png"}
]

const properties = [
    {"title":"Filtres", "elements":[
            {"name":"Age From", "type":"object", "renderAs":"textbox", "value":"90"
             , "properties":[
                 {"type":"dropdownlist", "label":"Display as", "values":["Textbox", "List"], "value":"List"}
                 ,{"type":"input", "label":"Prompt", "value":"Age From"}
                 ,{"type":"input", "label":"Default", "value":"min(age)"}
                 ,{"type":"input", "label":"Source", "value":"Patient[\"age\"]"}
                 ,{"type":"list", "label":"Connections", "values":["Table 1[\"age\"]", " Table 2[\"age\"]"]}
             ]}
            ,{"name":"Age To", "type":"object", "renderAs":"textbox", "value":"90"
             , "properties":[
                 {"type":"dropdownlist", "label":"Display as", "values":["Textbox", "List"], "value":"List"}
                 ,{"type":"input", "label":"Prompt", "value":"Age To"}
                 ,{"type":"input", "label":"Default", "value":"max(age)"}
                 ,{"type":"input", "label":"Source", "value":"Patient[\"age\"]"}
                 ,{"type":"list", "label":"Connections", "values":["Table 1[\"age\"]", " Table 2[\"age\"]"]}
             ]}
            ,{"name":"Pays","type":"object", "renderAs":"list", "value":"France", "values":["France", "Chili", "Espagne", "Catalogne"]
             , "properties":[
                 {"type":"dropdownlist", "label":"Display as", "values":["Textbox", "List"], "value":"List"}
                 ,{"type":"input", "label":"Prompt", "value":"Country"}
                 ,{"type":"input", "label":"Default", "value":"France"}
                 ,{"type":"input", "label":"Source", "value":"Patient[\"country\"]"}
                 ,{"type":"list", "label":"Connections", "values":["Table 1[\"countrycode\"]", " Table 2[\"countrycode\"]"]}
             ]}
            ,{"name":"Region","type":"object", "renderAs":"list" ,"value":"Bretagne", "values":[ "Ile-de-France", "Bretagne","Bourgogne-Franche-Comté","Aquitaine-Limousin-Poitou-Charentes","Normandy","Alsace-Champagne-Ardenne-Lorraine","Languedoc-Roussillon-Midi-Pyrénées","Nord-Pas-de-Calais-Picardie","Auvergne-Rhône-Alpes","Centre-Val de Loire","Corsica","French Guiana","Guadeloupe","Martinique","Mayotte","Pays de la Loire","Provence-Alpes-Côte d'Azur","Réunion"].sort()
             , "properties":[
                 {"type":"dropdownlist", "label":"Display as", "values":["Textbox", "List"], "value":"List"}
                 ,{"type":"input", "label":"Prompt", "value":"Region"}
                 ,{"type":"input", "label":"Default", "value":"Gretagne"}
                 ,{"type":"input", "label":"Source", "value":"Patient[\"Region\"]"}
                 ,{"type":"list", "label":"Connections", "values":["Table 1[\"regioncode\"]", " Table 2[\"regioncode\"]"]}
             ]}
         ]}
  ];


