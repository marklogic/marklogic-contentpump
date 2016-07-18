xquery version "1.0-ml" ;
module namespace m = "http://marklogic.com/module_invoke";

declare function m:lc( $list as node())
 as node()
{
  for $n in $list
  return typeswitch($n)
  case document-node() return document {
    m:lc($n/node()) }
  case element() return element {lower-case(fn:local-name($n)) } {
    $n/@*,
    m:lc($n/node())
  }
  default return $n
};

declare function m:uc( $list as node())
 as node()
{
  for $n in $list
  return typeswitch($n)
  case document-node() return document {
    m:uc($n/node()) }
  case element() return element {upper-case(fn:local-name($n)) } {
    $n/@*,
    m:uc($n/node())
  }
  default return $n
};

declare function m:transform($content as map:map, $context as map:map)
as map:map*
{
  let $result := map:map()
  let $result2 := map:map()
  let $uri := map:get($content, "uri")
  let $node := map:get($content, "value")
  let $dummy := map:put($result, "uri", $uri)
  let $dummy := map:put($result2, "uri", fn:concat($uri,map:get($context, "transform_param")))

  let $nodekind := xdmp:node-kind($node)
  let $dummy := xdmp:log(fn:concat("nodekind:",$nodekind))
  let $newVal := switch ($nodekind)
    case "binary" return $node
    case "document" return m:lc($node)
    case "element" return m:lc($node)
    default return $node
  let $newVal2 := switch ($nodekind)
    case "binary" return $node
    case "document" return m:uc($node)
    case "element" return m:uc($node)
    default return $node

  let $dummy := map:put($result, "value", $newVal)
  let $dummy := map:put($result2, "value", $newVal2)
  return ($result, $result2)

};
(:local:lc(doc($URI)):)
(:document {<root><a>hello</a>{doc($URI)}</root>}:)
