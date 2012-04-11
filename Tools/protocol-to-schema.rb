#!/usr/bin/env ruby

# for prot in "com.x.marketplace.v1"; do pushd $prot; for schema in *; do name=`echo $schema | sed -e"s#-.*##"`; version=`echo $schema | sed -e"s#.*-##" | sed -e"s#\^.*##"`; topic=`echo $schema | sed -e"s#.*^##"`; curl -XPOST -d`cat $schema` -k https://ocl.xcommercecloud.com/$prot/$name/$version/$topic; done; popd; done

require 'stringio'
require 'rubygems'
require 'avro'
require 'json'
require 'set'
require 'net/http'
require 'net/https'

def write_type(buf, schema, delim, written, namespace = nil)
  buf.print delim if delim

  if schema.class == Avro::Schema::UnionSchema
    buf.print '['
    delim = ''
    nullable = false
    schema.schemas.each do |subschema|
      nullable = nullable || (subschema.to_s == '"null"')
      write_type(buf, subschema, delim, written)
      delim = ','
    end
    buf.print ']'
    if nullable
      buf.print ',"default":null'
    end
  elsif schema.class == Avro::Schema::RecordSchema
    if !written.include?(schema.name)
      written.add(schema.name)

      namespace = schema.namespace if schema.namespace
      
      buf.print "{\"type\":\"record\",\"name\":\"#{schema.name}\""

      ['version', 'topic'].each do |prop|
        if schema.props.has_key? prop
          buf.print ", \"#{prop}\":\"#{schema.props[prop]}\""
        end
      end

      if namespace
        buf.print ",\"namespace\":\"#{namespace}\""
      end
      buf.print ",\"fields\":["
      
      delim = ''
      
      schema.fields.each do |field|
        buf.print delim
        buf.print "{\"name\":\"#{field.name}\",\"type\":"
        write_type(buf, field.type, '', written)

        field.props.each do |k,v|
          buf.print ", \"#{k}\": \"#{v}\""
        end

        buf.print '}'
        delim = ','
      end

      buf.print "]}"
    else
      if schema.namespace
        buf.print "\"#{schema.namespace}.#{schema.name}\""
      else
        buf.print "\"#{schema.name}\""
      end
    end
  elsif schema.class == Avro::Schema::ArraySchema
    buf.print "{\"type\":\"array\",\"items\":"

    write_type(buf, schema.items, '', written)

    buf.print "}"
  else
    json = schema.to_json()[1..-2].gsub('"\\"', '"').gsub('\\"', '"')
    buf.print json
  end
end

def process_protocol(protocol_file_name)
  protocol_file = File.open(protocol_file_name)
  protocol_text = protocol_file.read

  begin
    protocol = Avro::Protocol.parse(protocol_text)
  rescue
    $stderr.puts "Failed to parse #{protocol_file_name}"
    return
  end

  http = Net::HTTP.new('api.x.com', 443)
  http.use_ssl = true
#  http = Net::HTTP.new('localhost', 3000)
  
  protocol.types.each do |type|
    if type.class == Avro::Schema::RecordSchema && type.props['version']
      buf = StringIO.new
      
      written = Set.new
      
      print "publishing #{type.props['topic']} (#{type.name}) #{type.props['version']}... "
 
      namespace = type.namespace ? type.namespace : protocol.namespace

      write_type(buf, type, '', written, namespace)
      
      buf.rewind

      body = buf.read
      
      ["\n","\r","\t","\b"].each do |replace|
        body = body.gsub(replace, Regexp.escape(replace))
      end
      
#      out = File.open(namespace + '.' + type.name + '.avsc', 'w')
#      out.print body
#      out.close
      
      path = "/ocl/#{namespace}/#{type.name}/#{type.props['version']}"
      if type.props['topic']
        path += "?topic=#{type.props['topic'][1..-1]}&namespace=#{namespace}"
      end

      req = Net::HTTP::Post.new(path)
      req.basic_auth ARGV[0], ARGV[1]
      
      req.body = body
      resp, data = http.request req
      puts "done: #{resp.code} #{resp.code_type} #{data}"
    end
  end
end

if ARGV.length < 2
  fail("Usage: protocol-to-schema.rb <username> <password>")
end

jar_path = File.join(Dir.pwd, 'Tools', 'avro-tools-1.6.1.jar')

Dir['all/**/*'].each do |d|
  if File.directory? d
    Dir.chdir d do
      Dir['*'].each do |f|
        if f.match /\.avdl$/
          puts "Compiling #{f}"
          `java -jar #{jar_path} idl #{f} > #{f}.avpr`
          puts "Processing #{f}.avpr"
          process_protocol("#{f}.avpr")
        end
      end
    end
  end
end
