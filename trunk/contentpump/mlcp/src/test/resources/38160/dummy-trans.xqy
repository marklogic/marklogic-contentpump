xquery version "1.0-ml";

module namespace dm = "my.dummy.transform.module";

declare function dm:transform(
    $content as map:map,
    $context as map:map
  ) as map:map*
{
  let $uri := map:get($content, "uri")
  let $doc := map:get($content, "value")
  return
    (
      map:put($content, "uri", $uri),
      map:put($content, "value", $doc),
      $content
    )
};
