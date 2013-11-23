var fs = require('fs')
  , path = require('path')
  , mustache = require('mustache')
  , markdown = require('marked')
  , highlighter = require('highlight.js')

markdown.setOptions({
  gfm: true,
  highlight: function(code, lang) {
    if(lang)
      return highlighter.highlight(lang, code).value
    else
      return highlighter.highlightAuto(code).value
  }
});

function buildsite(template) {
  fs.readdir('content', function(err, files) {
    for(var i = 0; i < files.length; i++) {
      var file = files[i]
        , fullpath = path.join('content', files[i])
        , mdcontent = fs.readFileSync(fullpath, 'utf8')
        , htmlfilename = file.replace(/markdown/, "html")
        , content = markdown(mdcontent)
      fs.writeFileSync(htmlfilename, template(content) , 'utf8')
    }
  })
}

function run() {
  var template = fs.readFileSync('template.html', 'utf8')
  buildsite(function(content) {
    return mustache.render(template, { content: content })
  })
}

run()
