defmodule Dispatcher do
  use Matcher
  define_accept_types [
    html: [ "text/html", "application/xhtml+html" ],
    json: [ "application/json", "application/vnd.api+json" ],
    sparql: [ "application/sparql-results+json" ]
  ]

  @any %{}
  @json %{ accept: %{ json: true } }
  @html %{ accept: %{ html: true } }
  @sparql %{ accept: %{ sparql: true } }

  # In order to forward the 'themes' resource to the
  # resource service, use the following forward rule:
  #
  # match "/themes/*path", @json do
  #   Proxy.forward conn, path, "http://resource/themes/"
  # end
  #
  # Run `docker-compose restart dispatcher` after updating
  # this file.

  options "/*_path" do
    send_resp( conn, 200, "Option calls are accepted by default")
  end

  match "/*path", @html do
    forward conn, path, "http://frontend/"
  end

  match "/query-equivalence/*path", @json do
    forward conn, path, "http://query-equivalence/"
  end

  match "/sparql/*path", @sparql do
    forward conn, path, "http://database:8890/sparql/"
  end

  match "/*_", %{ last_call: true } do
    send_resp( conn, 404, "Route not found.  See config/dispatcher.ex" )
  end
end
