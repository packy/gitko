package XMLUtils;
use strict;
use warnings;
####################################################################
#
# NOTE: loads xerces.dll/so. Add dagent's dir
# to $LD_LIBRARY_PATH or %PATH% so DynaLoader can find it.
#
####################################################################
use XML::Xerces;
use Data::Dumper;
use Hash::Util;

use FileUtils;
###################################################################
#
# XML to perl data, similar to XML::Simple. Traverse xml in
# perland.
#
###################################################################
sub xml_parse {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $doc  = __xml_real_parse($args{xml});
    my $root = $doc->getDocumentElement();

    my $ret = {};
    __xml_process_tree($root, \$ret);

#    print STDERR Dumper($ret);

    return $ret;
}
###################################################################
#
#    Will call a sub on each node in the document, matching the
#    taganame specified.
#
# args:
#
#    xml  => filename
#    name => tag name
#    callback => coderef
#    user_data => extra data to pass to callback
#
###################################################################
sub xml_find_element_by_tag_name {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $doc = __xml_real_parse($args{xml});

    foreach my $elem ($doc->getElementsByTagName($args{name})) {

        $args{callback}->($elem, $args{user_data});
    }
}
##################################################################
#
# private
#
##################################################################
sub __xml_real_parse {

    my ($xml) = @_;

    my $parser = XML::Xerces::XercesDOMParser->new();
    $parser->setErrorHandler(XMLUtils::ErrorHandler->new());
    #
    # Some errors occur outside parsing and are not caught by the
    # parser's ErrorHandler. XML::Xerces provides a way for catching
    # these errors using the PerlExceptionHandler class. Usually the
    # following code is enough for catching exceptions:
    #
    #   eval{$parser->parser($my_file)};
    #   XML::Xerces::error($@) if $@;
    #
    eval {

        $parser->parse($xml);
    };

    if ($@) {

        FileUtils::fatal("XML parse error: " . XML::Xerces::error($@));
    }

    my $doc = $parser->getDocument();

    return $doc;
}

sub __xml_process_tree {

    my ($root, $ptr) = @_;

    my $child = $root->getFirstChild();

    while (defined $child) {

        my $type = $child->getNodeType();

        if ($type == $XML::Xerces::DOMNode::ELEMENT_NODE) {

            my $name = $child->getNodeName();
            my %attr = $child->getAttributes();

            $$ptr->{$name} = {%attr,
                              name => $name };

            __xml_process_tree($child, \$$ptr->{$name});

        } elsif ($type == $XML::Xerces::DOMNode::TEXT_NODE) {

            my $value = $child->getTextContent();
            #
            # cleanup text
            #
            $value =~ s/^\s+//;
            $value =~ s/\s+$//;
            chomp $value;
            
            if ($value) {

                $$ptr->{value} = $value;
            }

        } elsif ($type == $XML::Xerces::DOMNode::CDATA_SECTION_NODE) {
            #
            # Don't process
            #
            $$ptr->{value} = $child->getTextContent();

        } else {

            FileUtils::log("[WARNING] Unhandled Node type " . $type);
        }

        $child = $child->getNextSibling();
    }
}
##################################################################
#
# Xerces perl error handler
#
##################################################################
package XMLUtils::ErrorHandler;
@XMLUtils::ErrorHandler::ISA = qw(XML::Xerces::PerlErrorHandler);

sub fatal_error {

    my ($self, $error) = @_;

    FileUtils::fatal("Xerces error: [$error]");
}
        
1;
__END__
