/*
 * Client-side Java syntax highlighting for the Javadoc's `<pre>{@code ...}</pre>`
 * examples. The JDK 17 doclet has no --add-script, so the build injects this file
 * inline through the -bottom option (with --allow-script-in-comments); it therefore
 * runs after every page's content exists and needs no load event. Tokens get
 * `hl-*` classes colored by javadoc-theme.css. Blocks that already contain markup
 * are left untouched, so doclet-generated links inside snippets survive.
 */
(function () {
  "use strict";
  var TOKEN = new RegExp(
    [
      "(\\/\\*[\\s\\S]*?\\*\\/|\\/\\/[^\\n]*)", // 1: comment
      '("""[\\s\\S]*?"""|"(?:[^"\\\\\\n]|\\\\.)*"|\'(?:[^\'\\\\\\n]|\\\\.)*\')', // 2: string or char
      "(@[A-Za-z_][A-Za-z0-9_]*)", // 3: annotation
      "(\\b\\d(?:[\\w.]|_(?=\\w))*\\b)", // 4: number
      "(\\b(?:abstract|assert|boolean|break|byte|case|catch|char|class|const|" +
        "continue|default|do|double|else|enum|extends|final|finally|float|for|" +
        "goto|if|implements|import|instanceof|int|interface|long|native|new|" +
        "package|permits|private|protected|public|record|return|sealed|short|" +
        "static|strictfp|super|switch|synchronized|this|throw|throws|transient|" +
        "try|var|void|volatile|while|yield|true|false|null)\\b)", // 5: keyword
    ].join("|"),
    "g"
  );
  var CLASSES = ["hl-comment", "hl-string", "hl-annotation", "hl-number", "hl-keyword"];

  function highlight(code) {
    if (code.children.length > 0) {
      return;
    }
    var text = code.textContent;
    var fragment = document.createDocumentFragment();
    var last = 0;
    var match;
    TOKEN.lastIndex = 0;
    while ((match = TOKEN.exec(text)) !== null) {
      if (match.index > last) {
        fragment.appendChild(document.createTextNode(text.slice(last, match.index)));
      }
      var span = document.createElement("span");
      for (var group = 1; group <= CLASSES.length; group++) {
        if (match[group] !== undefined) {
          span.className = CLASSES[group - 1];
          break;
        }
      }
      span.textContent = match[0];
      fragment.appendChild(span);
      last = match.index + match[0].length;
    }
    if (last === 0) {
      return;
    }
    fragment.appendChild(document.createTextNode(text.slice(last)));
    code.textContent = "";
    code.appendChild(fragment);
  }

  var blocks = document.querySelectorAll("pre > code, div.snippet-container pre");
  for (var index = 0; index < blocks.length; index++) {
    highlight(blocks[index]);
  }
})();
