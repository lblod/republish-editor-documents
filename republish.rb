#!/usr/bin/env ruby
# coding: utf-8

STDOUT.sync = true

require 'linkeddata'
require 'date'
require 'securerandom'
require 'tempfile'
require 'csv'
require 'pry-byebug'
require 'digest/sha1'
require 'httpclient'

class Republisher
  attr_reader :client, :log

  ORG = RDF::Vocab::ORG
  FOAF = RDF::Vocab::FOAF
  SKOS = RDF::Vocab::SKOS
  DC = RDF::Vocab::DC
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
  PERSON = RDF::Vocabulary.new("http://www.w3.org/ns/person#")
  PERSOON = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/persoon#")
  MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
  BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
  EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  BASE_IRI='http://data.lblod.info/id'

  DOCSTATES = { "besluitenlijst publiek" => "http://mu.semte.ch/application/editor-document-statuses/b763390a63d548bb977fb4804293084a",
                "prullenbak" => "http://mu.semte.ch/application/editor-document-statuses/5A8304E8C093B00009000010",
                "agenda publiek" => "http://mu.semte.ch/application/editor-document-statuses/627aec5d144c422bbd1077022c9b45d1",
                "goedgekeurd" => "http://mu.semte.ch/application/editor-document-statuses/c272d47d756d4aeaa0be72081f1389c6"}

  def initialize(endpoint, publish_endpoint)
    @endpoint = endpoint
    @publish_endpoint = publish_endpoint
    options = {}
    options[:read_timeout] = 360
    @client = SPARQL::Client.new(endpoint, options)
    @log = Logger.new(STDOUT)
    @log.level = Logger::INFO
    wait_for_db
    @manual_check = []
    @published_status_no_publication = []
    @failed_deletes = []
    @failed_publish = []
    @ok_publish = []
  end

  def run()
    docs_info = find_docs_to_republish()
    docs_info.each do |triple|
      has_delete_errors = cleanup_published_page(triple)
      if has_delete_errors
        next
      end
      republish(triple)
    end

    write_log
  end

  def find_docs_to_republish()
    graphs = find_graphs_with_doc()
    docs_info = []
    graphs.each do |graph|
      triples = find_published_docs(graph.g.value)
      p "Found #{triples.length} docs"
      if(triples.length == 0)
        p "No triples found for #{graph.g.value}"
        next
      end
      triple = find_sane_doc_to_publish(triples)
      if triple
        docs_info << triple
      end
    end
    p "Found #{docs_info.length} to publish"
    docs_info
  end

  def cleanup_published_page(triple)
    has_errors = false
    triples = find_zittingen_linked_to_eenheid(triple.eenheid.value)
    p "Found #{triples.length} docs"
    if(triples.length == 0)
      p "No zitting found for #{triple.eenheidNaam.value}, #{triple.eenheidType.value}"
      @published_status_no_publication << triple
      return
    end

    triples.each do |zitting|
      begin
       cleanup_zitting(zitting)
       p "Cleaned #{zitting.eenheidNaam.value}, #{zitting.eenheidType.value} #{zitting.zitting.value}"
      rescue
        @failed_deletes << zitting
        has_errors = true
      end
    end
    has_errors
  end

  def cleanup_zitting(triple)
    remove_zitting_with_besluiten(triple.zittingId.value)
    remove_zitting_with_agenda_only(triple.zittingId.value)
  end

  def republish(triple)
    p "Starting publish"
    endpoint = @publish_endpoint
    agenda = endpoint + "/publish/agenda/#{triple.docId.value}"
    besluiten = endpoint + "/publish/decision/#{triple.docId.value}"
    notulen = endpoint + "/publish/notule/#{triple.docId.value}"

    inverted_mapping = DOCSTATES.invert

    begin
      if inverted_mapping[triple.status.value] == "goedgekeurd"
        post(agenda)
        post(besluiten)
        post(notulen)
        p " publishing ok for EENHEID: #{triple.eenheidNaam.value}, TYPE #{triple.eenheidType.value}"
        @ok_publish << triple
        return
      end

      if  inverted_mapping[triple.status.value] == "besluitenlijst publiek"
        post(agenda)
        post(besluiten)
        p " publishing ok for EENHEID: #{triple.eenheidNaam.value}, TYPE #{triple.eenheidType.value}"
        @ok_publish << triple
        return
      end

      if  inverted_mapping[triple[-1].status.value] == "agenda publiek"
        post(agenda)
         p " publishing ok for EENHEID: #{triple.eenheidNaam.value}, TYPE #{triple.eenheidType.value}"
        @ok_publish << triple
        return
      end
    rescue
      p " publishing failed for EENHEID: #{triple.eenheidNaam.value}, TYPE #{triple.eenheidType.value}"
      @failed_publish << triple
      has_error = true
    end

  end

  def post(endpoint)
    client = HTTPClient.new
    method = 'POST'
    url = URI.parse endpoint
    res = client.request method, url
    if not 200 >= res.status_code and not res.status_code < 300
      binding.pry
      raise "Error publishing #{endpoint}"
    end
  end

  def find_graphs_with_doc()
    query(%(
          PREFIX pav: <http://purl.org/pav/>
          PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

          SELECT DISTINCT ?g
          WHERE {
            GRAPH ?g {
              ?s a ext:EditorDocument.
           }
         }
       ))
  end

  def find_zittingen_linked_to_eenheid(eenheid)
    # a zitting is implied they published something
    query(%(
          PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
          PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
          PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
          PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
          PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>

          SELECT DISTINCT ?zitting ?zittingId ?eenheid ?eenheidId ?eenheidNaam ?eenheidType
          WHERE {
              GRAPH <http://mu.semte.ch/graphs/public> {
                ?zitting mu:uuid ?zittingId.
                ?zitting a besluit:Zitting.
                ?zitting besluit:isGehoudenDoor ?orgaanInTijd.
                ?orgaanInTijd mandaat:isTijdspecialisatieVan ?orgaan.
                ?orgaan besluit:bestuurt <#{eenheid}>.
                <#{eenheid}> skos:prefLabel ?eenheidNaam.
                <#{eenheid}> mu:uuid ?eenheidId.
                <#{eenheid}> besluit:classificatie ?classS.
                ?classS skos:prefLabel ?eenheidType.
             }
           }
       ))
  end

  def remove_zitting_with_besluiten(zittingUid)
    # removes notule too
    query_str = %(
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX eli: <http://data.europa.eu/eli/ontology#>

    DELETE {
     GRAPH <http://mu.semte.ch/graphs/public> {
       ?s ?p ?o.
       ?agenda ?agendaP ?agendaO.
       ?agendapunt ?agendapuntP ?agendapuntO.
       ?bav ?bavP ?bavO.
       ?besluit ?besluitP ?besluitO.
       ?artikel ?artikelP ?artikelO.
     }
    }
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ?s mu:uuid "#{zittingUid}". #zitting uuid
        ?s ?p ?o.
        ?s besluit:heeftAgenda ?agenda.
        ?agenda ?agendaP ?agendaO.
        ?agenda besluit:heeftAgendapunt ?agendapunt.
        ?agendapunt ?agendapuntP ?agendapuntO.
        ?bav dct:subject ?agendapunt.
        ?bav ?bavP ?bavO.
        ?bav prov:generated ?besluit.
        ?besluit ?besluitP ?besluitO.
        ?besluit eli:has_part ?artikel.
        ?artikel ?artikelP ?artikelO.
      }
    }
  )
    res = query(query_str)
  end

  def remove_zitting_with_agenda_only(zittingUid)
    query_str = %(
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX eli: <http://data.europa.eu/eli/ontology#>

    DELETE {
     GRAPH <http://mu.semte.ch/graphs/public> {
       ?s ?p ?o.
       ?agenda ?agendaP ?agendaO.
       ?agendapunt ?agendapuntP ?agendapuntO.
     }
    }
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ?s mu:uuid "#{zittingUid}". #zitting uuid
        ?s ?p ?o.
        ?s besluit:heeftAgenda ?agenda.
        ?agenda ?agendaP ?agendaO.
        ?agenda besluit:heeftAgendapunt ?agendapunt.
        ?agendapunt ?agendapuntP ?agendapuntO.
      }
    }
  )
   res = query(query_str)
  end

  def find_published_docs(eenheid_g)
    uuid = eenheid_g.gsub("http://mu.semte.ch/graphs/organizations/", "")
    query_str = %(
            PREFIX pav: <http://purl.org/pav/>
            PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
            PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
            PREFIX dct: <http://purl.org/dc/terms/>

            SELECT DISTINCT ?doc ?docId ?modified ?eenheidType ?eenheidNaam ?statusName ?status ?content ?title ?eenheid ?eenheidId
            WHERE {
              GRAPH <http://mu.semte.ch/graphs/public> {
                ?eenheid mu:uuid "#{uuid}".
                ?eenheid mu:uuid ?eenheidId.
                ?eenheid skos:prefLabel ?eenheidNaam.
                ?eenheid besluit:classificatie ?classS.
                ?classS skos:prefLabel ?eenheidType.
                ?status ext:EditorDocumentStatusName ?statusName
               }


              GRAPH <#{eenheid_g}> {
                ?doc a ext:EditorDocument.
                ?doc ext:editorDocumentStatus ?status.
                ?doc pav:lastUpdateOn ?modified.
                ?doc dct:title ?title.
                ?doc mu:uuid ?docId.
                ?doc ext:editorDocumentContent ?content.
                FILTER(
                  NOT EXISTS {
                       ?prevV pav:previousVersion ?doc.
                  }
                )
                FILTER (?status in (<#{DOCSTATES["agenda publiek"]}>,
                                    <#{DOCSTATES["besluitenlijst publiek"]}>,
                                    <#{DOCSTATES["goedgekeurd"]}>))
              }
            }
            ORDER BY ?modified
         )
    query(query_str)
  end

  def find_sane_doc_to_publish(triples)
    ########################################################################
    # Assumes triples are sorted by date
    ########################################################################
    inverted_mapping = DOCSTATES.invert

    if(not triples.length == (triples.uniq{ |t| t.doc.value }).length)
      raise "duplicate doc uri found. Sure query correct?"
    end

    if inverted_mapping[triples[-1].status.value] == "goedgekeurd"
      if(not all_same_docs(triples))
        @manual_check << triples[-1] # if not the case a manual check should be performed
        return nil
      end
       p "Last entry (notulen) is valid for #{triples[-1].eenheidNaam.value}"
      return triples[-1]
    end

    has_goedgekeurd = triples.find{ |t| inverted_mapping[t.status.value] == "goedgekeurd" }

    # last document modified is besluitenlijst publiek remove other docs
    if  inverted_mapping[triples[-1].status.value] == "besluitenlijst publiek" and not has_goedgekeurd
      if(not all_same_docs(triples))
        @manual_check << triples[-1] # if not the case a manual check should be performed
        return nil
      end
       p "Last entry (besluiten) is valid for #{triples[-1].eenheidNaam.value}"
      return triples[-1]
    end

    if inverted_mapping[triples[-1].status.value] == "besluitenlijst publiek" and has_goedgekeurd
      @manual_check << triples[-1]
      return nil
    end

    has_besluitenlijst = triples.find{ |t| inverted_mapping[t.status.value] == "besluitenlijst publiek" }

    if  inverted_mapping[triples[-1].status.value] == "agenda publiek" and not has_besluitenlijst and not has_goedgekeurd
      if(not all_same_docs(triples))
        @manual_check << triples[-1] # if not the case a manual check should be performed
        return nil
      end
      p "Last entry (agenda) is valid for #{triples[-1].eenheidNaam.value}"
      return triples[-1]
    end

    #Here we arrive in some weird state better see that is happening
    @manual_check << triples[-1]
    nil
  end

  def all_same_docs(triples)
    (triples.uniq{ |t| t.title.value }).length == 1
  end

  def write_log
    file_path = File.join(ENV['OUTPUT_PATH'],"#{DateTime.now.strftime("%Y%m%d%H%M%S")}-publish.log")
    open(file_path, 'w') { |f|
      f << "!!!!!!! Some weird states to check (order or title is not ok):"

      @manual_check.each do |t|
        f << "- EENHEID: #{t.eenheidNaam.value}, TYPE #{t.eenheidType.value}"
      end

      f <<  "!!!!!!! Published in notule but didn't hit besluiten"
      @published_status_no_publication.each do |t|
       f << "- EENHEID: #{t.eenheidNaam.value}, TYPE #{t.eenheidType.value}"
      end

      f << "!!!!!!! Failed remove publication for zitting:"
      @failed_deletes.each do |t|
       f  << "- EENHEID: #{t.eenheidNaam.value}, TYPE #{t.eenheidType.value}"
      end

      f << "!!!!!!! Failed doc publish for:"
      @failed_publish.each do |t|
       f  << "- EENHEID: #{t.eenheidNaam.value}, TYPE #{t.eenheidType.value}"
      end

      f << "List of all failed publish and delete eenheden"
      f << (@failed_delete + @failed_publish).reduce([]){ |acc, t| acc << "#{t.eenhedId.value}" }

      f << "list of eenheden ok publish"
      @ok_publish.each do |t|
       f  << "- EENHEID: #{t.eenheidNaam.value}, TYPE #{t.eenheidType.value}"
      end

    }

  end

  def query(q)
    log.debug q
    @client.query(q)
  end

  def wait_for_db
    until is_database_up?
      log.info "Waiting for database... "
      sleep 2
    end

    log.info "Database is up"
  end

  def is_database_up?
    begin
      location = URI(@endpoint)
      response = Net::HTTP.get_response( location )
      return response.is_a? Net::HTTPSuccess
    rescue Errno::ECONNREFUSED
      return false
    end
  end

end


mdb = Republisher.new(ENV['ENDPOINT'], ENV['PUBLISHENDPOINT'])
mdb.run()
