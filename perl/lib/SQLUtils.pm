package SQLUtils;

use strict;
use warnings;
use Carp;
use FileUtils;
use File::Spec;
use Data::Dumper;

our @EXPORT_OK = qw(selectrow_array selectrow_arrayref selectall_arrayref type);
our ($type, $debug);

use constant {
        types => [qw(oracle mssql sybase db2)]
};

# attributes of the SQLUtils object
use constant FIELDS => [qw(type debug)];
use constant FILTER_EXPR => '###DUMMY+DATA###';
use constant FILTER_PATH => File::Spec->catfile($ENV{GA_SCRIPT_DIR}, 'filter.dat');

sub new {
    my $class = shift;
    my $type = ref $class || $class;

    my %args = @_;


    my $self = bless {}, $class;
    $self->_init(\%args);
    return $self;
}


sub selectrow_array {
    my $self = ref($_[0]) eq 'SQLUtils' ? shift : undef;
    push @_, {'feedback_off' => 1};
    return @{_select(@_)->[0]};
}

sub selectrow_arrayref {
    my $self = ref($_[0]) eq 'SQLUtils' ? shift : undef;
    push @_, {'feedback_off' => 1};
    return _select(@_)->[0];
}

sub selectall_arrayref {
    my $self = ref($_[0]) eq 'SQLUtils' ? shift : undef;
    push @_, {'feedback_off' => 1};
    return _select(@_);
}

sub type {
    my $self = ref($_[0]) eq 'SQLUtils' ? shift : undef;
    my $type = shift;
    if ( $type ) {
       $SQLUtils::type = $type;
       _map_type();

    }
    return $SQLUtils::type;
}

sub enable_filter {
    my $self = ref($_[0]) eq 'SQLUtils' ? shift : undef;
    my $dummy_query = qq/SELECT '/ . FILTER_EXPR . qq/' AS FILTER FROM dual;/;
    my $result = _execute($dummy_query, feedback_off => 1);

    local $/;
    open my $fc, "<:raw", $result->{stdout};
    my $content = <$fc>;
    close $fc;

    my $known_content = quotemeta(FILTER_EXPR);
    $content =~ s/$known_content//smx;
    chop($content);

    _debug_out([caller(0)], {content => $content, filtered => $known_content}) if $SQLUtils::debug;

    _set_filter($content) if ( $content && $content =~ /\S/ );
}

# +++++++++++++++++++ PRIVATE SECTION ++++++++++++++++++++++++

sub _init {
    my $self = shift;
    my $args = shift;
    no strict 'refs';
    foreach my $field ( @{+FIELDS} ) {
        $self->{$field} = undef;
        $self->{$field} = $args->{$field} if exists $args->{$field};
        ${"SQLUtils::$field"} = $args->{$field};
    }

    unless ( grep { $self->{type} eq $_ } @{+types} ) {
        croak "Can't create object of unknown type [$self->{type}] !";
    }

    _map_type();
}


sub _sanitize_output {

    _debug_out([caller(0)], { args => \@_ }) if $SQLUtils::debug;

    my $content_path = shift;
    my $phrase = shift;

    local $/;
    open my $rh, "<:raw", $content_path;
    my $content = <$rh>;
    close $rh;

    $content =~ s/\Q$phrase\E//smx if $phrase;

    open my $wh, ">:raw", $content_path;
    binmode $wh;
    print $wh $content;
    close $wh;
}


sub _parse_rows {
    my $output = shift;
    my @rows;
    _sanitize_output($output, _load_filter());

    FileUtils::file_foreach_line(
        $output,
        sub {
            my $line = shift;
            return 1 unless ( $line && $line =~ /\S/ );
            my @tuple = split(/\|/, $line);
            @tuple = map { FileUtils::trim($_) } @tuple;
            push @rows, \@tuple;
            return 1;
        }
    );

    return \@rows;
}

sub _select {
    my $query = shift;
    my $args = shift;
    my $result = _execute($query, %$args);
    return _parse_rows($result->{stdout});
}

sub _execute {}

sub _map_type {

    unless ( grep { $SQLUtils::type eq $_ } @{+types} ) {
        croak "Can't map query for unknown SQL type [$SQLUtils::type] !\n Please, use following types: 'oracle', 'mssql', 'sybase'.";
    }

   SWITCH: {
       $SQLUtils::type eq 'oracle' && do {require OracleUtils; *SQLUtils::_execute = \&OracleUtils::oracle_run_sql; last;};
       $SQLUtils::type eq 'mssql' && do {require MSSQLUtils; *SQLUtils::_execute = \&MSSQLUtils::mssql_run_sql; last;};
       $SQLUtils::type eq 'sybase' && do {require SybaseUtils; *SQLUtils::_execute = \&SybaseUtils::execute; last;};
   };
}

sub _set_filter {
    my $phrase = shift || undef;

    if ( $phrase ) {
        open my $fh, ">:raw", FILTER_PATH;
        print $fh $phrase;
        close $fh;
    }
}

sub _load_filter {
    my $content;
    if ( -e FILTER_PATH ) {
        open my $fh, "<:raw", FILTER_PATH;
        local $/;
        $content = <$fh>;
        close $fh;
    }
    return $content;
}

sub _debug_out {
    my $caller = shift;
    my $data = shift;

    print "\n" . "="x100 . "\n";
    print "Function $caller->[3]\n" ;
    print Dumper($data) . "\n";
    print "\n" . "="x100 . "\n";
}

1;
