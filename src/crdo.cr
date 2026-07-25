require "./crdo/all"

{% unless flag?(:crdo_spec) %}
  main
{% end %}
