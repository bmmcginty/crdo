require "./src/crdo/all"

{% unless flag?(:crdo_spec) %}
main
{% end %}
