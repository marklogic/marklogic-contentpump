xquery version "1.0-ml";

module namespace dmc = "dmc";

declare function dmc:transform(
  $content as map:map,
  $context as map:map)
as map:map*
{
  map:put($content, 'uri', fn:replace(map:get($content, 'uri'), '.0', '.xml')),
  
  map:put($content, 'value',
    xdmp:unquote(
      xdmp:quote(map:get($content, 'value'))
    )
  ),
  
  $content
};
