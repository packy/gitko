package OpatchUtils;
use strict;
use warnings;
use Data::Dumper;
use Hash::Util;

use FindBin qw($Bin);
use lib "$Bin/lib";
use FileUtils;

use constant IS_WINDOWS => ($^O eq 'MSWin32' ? 1 : 0);

###########################################################################
#
# Opatch output parsers
#
###########################################################################
my %OPATCH_PARSER = (
    # 9.2 / 10.1
    '1.0' => {
        oracle_version => \&_opatch_oracle_version_9_2,
        oracle_patches => \&_opatch_installed_patches_9_2,
        oracle_products=> \&_opatch_installed_products_9_2,

    },
    '10.2' => {
        oracle_version => \&_opatch_oracle_version_10_2,
        oracle_patches => \&_opatch_installed_patches_10_2,
        oracle_products=> \&_opatch_installed_products_10_2,
    },
    '11.1' => {
        oracle_version => \&_opatch_oracle_version_10_2,
        oracle_patches => \&_opatch_installed_patches_10_2,
        oracle_products=> \&_opatch_installed_products_10_2,
    },
    '11.2' => {
        oracle_version => \&_opatch_oracle_version_10_2,
        oracle_patches => \&_opatch_installed_patches_10_2,
        oracle_products=> \&_opatch_installed_products_10_2,
    },
);
###############################################################################
#
# opatch lsinventory -patch
#
###############################################################################
sub opatch_installed_patches
{

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $out = _opatch_run('lsinventory', '-patch', %args);
    #
    # find opatch version
    #
    my $opatch_version = _opatch_get_version($out);

    if (!exists $OPATCH_PARSER{$opatch_version})
    {
        FileUtils::fatal("opatch [$opatch_version] is not supported.");
    }
    #
    # ... and use the apropriate parser
    #
    return $OPATCH_PARSER{$opatch_version}->{oracle_patches}->($out);
}
###############################################################################
#
# opatch lsinventory -detail
#
###############################################################################
sub opatch_installed_products {

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $out = _opatch_run('lsinventory', '-detail', %args);
    #
    # find opatch version
    #
    my $opatch_version = _opatch_get_version($out);

    if (!exists $OPATCH_PARSER{$opatch_version}) {

        FileUtils::fatal("opatch [$opatch_version] is not supported.");
    }
    #
    # ... and use the apropriate parser
    #
    return $OPATCH_PARSER{$opatch_version}->{oracle_products}->($out);
}
#################################################################################
#
# get the oracle version from opatch
#
#################################################################################
sub opatch_oracle_version
{

    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $out = _opatch_run('lsinventory', '', %args);
    #
    # find out opatch version first
    #
    my $opatch_version = _opatch_get_version($out);

    if (!exists $OPATCH_PARSER{$opatch_version})
    {
        FileUtils::fatal("opatch [$opatch_version] is not supported.");
    }

    return $OPATCH_PARSER{$opatch_version}->{oracle_version}->($out);
}

sub opatch_version
{
    my (%args) = @_;
    Hash::Util::lock_keys(%args);

    my $out = _opatch_run('version', '', %args);
    #
    #OPatch Version: 10.2.0.4.3
    #
    my $cb = sub {

        my ($line, $version) = @_;

        if ($line =~ /^OPatch Version\: (\d+\.\d+\.\d+\.\d+\.\d+)/i)
        {
            $$version = $1;
            return 0;
        }

        return 1;
    };

    my $opatch_version;
    FileUtils::file_foreach_line($out, $cb, \$opatch_version);

    if (!defined $opatch_version || !$opatch_version)
    {
        FileUtils::fatal("cannot find opatch version.");
    }

    return $opatch_version;
}
#
# private subs
#
###############################################################################
#
# run opatch with args
#
###############################################################################
sub _opatch_run
{

    my ($main, $extra, %args) = @_;
    Hash::Util::lock_keys(%args);

    my $out;
    my $opatch_cmd = File::Spec->catfile($args{oracle_home}, 'OPatch', IS_WINDOWS ? 'opatch.bat' : 'opatch');
    my $loc_file = IS_WINDOWS ?
        FileUtils::win32_registry_read_key('HKEY_LOCAL_MACHINE/SOFTWARE/oracle/', 'inst_loc') :
        File::Spec->catfile($args{oracle_home}, 'oraInst.loc');

    if (!exists $args{rhost})
    {
        $out = FileUtils::file_execute(
            cmd  => $opatch_cmd,
            args => [
                $main,
                '-oh', $args{oracle_home},
                qq/'-invPtrLoc $loc_file'/, '-retry', '10', '-delay', '30', $extra
            ],
            env    => {ORACLE_HOME => $args{oracle_home},
            },
            user   => $args{user},
            dir    => $args{oracle_home},
#            expect => qr/^OPatch succeeded\./i,
        );
    }
    else
    {
        #
        # remote call
        #
        $out = FileUtils::file_execute(
            cmd  => $opatch_cmd,
            args => [
                $main,
                '-oh', $args{oracle_home},
                q/'-invPtrLoc $loc_file'/, '-retry', '10', '-delay', '30', $extra
            ],
            renv   => {ORACLE_HOME => $args{oracle_home},
            },
            ruser  => $args{ssh_as_root} ? $args{user} : undef,
            user   => $args{ssh_as_root} ? undef : $args{user},
            rhost  => $args{rhost},
            rdir   => $args{oracle_home},
#            expect => qr/^OPatch succeeded\./i,
        );
    }

    return $out->{stdout};
}
###############################################################################
#
# Oracle 9.2 opatch output parser
#
###############################################################################
sub _opatch_installed_patches_9_2
{

    my $file = shift;

    my $patch_id;

    my $callback = sub {

        my ($line, $patches) = @_;

        if (!$line)
        {
            $patch_id = '';
            return 1;
        }

        if ($line =~
            /\d+\) Patch (\d+) applied on (\w{3}) (\w{3}) (\d{1,2}) (\d{1,2})\:(\d{1,2})\:(\d{1,2}) (\w{3,}) (\d{4})$/
          )
        {
            $patch_id = $1;
            $patches->{$patch_id}->{applied} = "$4 $3 $9";

        }
        elsif ($line =~ /\[ Base Bug\(s\)\: (.*)\]/)
        {
            my $match = $1;
            $patches->{$patch_id}->{fixes} = [split /\s/, $match];
        }

        return 1;
    };

    my %patches;
    FileUtils::file_foreach_line($file, $callback, \%patches);
    return %patches;
}
#########################################################################
#
# Oracle 10.2 opatch output parser
#
##########################################################################
sub _opatch_installed_patches_10_2
{

    my $file = shift;

    my $patch_id;

    my $callback = sub {

        my ($line, $patches) = @_;

        if (!$line)
        {
            $patch_id = '';
            return 1;
        }

        if ($line =~
            /^Patch\s+(\d+)\s+\: applied on (\w{3}) (\w{3}) (\d{1,2}) (\d{1,2})\:(\d{1,2})\:(\d{1,2}) (\w{3,}) (\d{4})$/
          )
        {

            $patch_id = $1;
            $patches->{$patch_id}->{applied} = "$4 $3 $9";

        }
        elsif ($line =~ /^\s+Created on (\d{1,2}) (\w{3}) (\d{4})\,/)
        {

            $patches->{$patch_id}->{created} = "$1 $2 $3";

        }
        elsif ($line =~ /^\s+(\d+.*)$/)
        {

            my $match = $1;
            push @{$patches->{$patch_id}->{fixes}}, (split /\,\s*/, $match);
        }
        else
        {
            # flooding the log...
            # print "[DEBUG]: ignoring line [$line]\n";
        }
	return 1;
    };

    my %patches;
    FileUtils::file_foreach_line($file, $callback, \%patches);

    FileUtils::log("Installed patches " . Dumper(\%patches));

    return %patches;
}
################################################################################
#
# find opatch version in opatch output
#
################################################################################
sub _opatch_get_version
{

    my $file = shift;

    my $cb = sub {

        my ($line, $version) = @_;

        if ($line =~ /Oracle Interim Patch Installer version (\d+\.\d+)\./i)
        {
            $$version = $1;
            return 0;
        }

        return 1;
    };

    my $version;
    FileUtils::file_foreach_line($file, $cb, \$version);

    if (!defined $version)
    {
        FileUtils::fatal("cannot determine opatch version.");
    }

    return $version;
}
################################################################################
#
# find oracle version in opatch output
#
################################################################################
sub _opatch_oracle_version_9_2
{
    #
    # don't see oracle version here
    #
    my $out = shift;

    return '9.2';
}
################################################################################
#
# find oracle version in opatch output
#
################################################################################
sub _opatch_oracle_version_10_2
{

    my $file = shift;
#
#Installed Top-level Products (1):
#
#Oracle Database 11g                                                  11.2.0.1.0
#There are 1 products installed in this Oracle Home.
#
    my $callback = sub {

        my ($line, $version) = @_;

        if ($line =~ /There are \d+ products installed/i)
        {
            return 0;
        }
        elsif ($line =~ /Oracle Database .* (\d{1,2}\.\d\.\d\.\d\.\d)$/i)
        {
            push @$version, $1;
        }
        elsif ($line =~
            /Oracle Grid Infrastructure .* (\d{1,2}\.\d\.\d\.\d\.\d)$/i)
        {
            push @$version, $1;
        }

        return 1;
    };

    my @version = ();
    FileUtils::file_foreach_line($file, $callback, \@version);

    if (!scalar @version)
    {
        FileUtils::fatal("cannot find Oracle version.");
    }
    #
    # The last element has the last PatchSet
    #
    return pop @version;
}

sub _opatch_installed_products_10_2 {

    my $file = shift;
    my $in   = 0;

    my %ret = ();

    my $callback = sub {

        my $line = shift;

        return 1
            unless $line;

        if (!$in) {

            $in = ($line =~ /^Installed Products \(\d+\)\:/);
            return 1;
        }

        return 0
            if $line =~ /There are \d+ products installed in this Oracle Home\./;

        my @data = split(/\s{3,}/, $line, 2);
        # component => version
        push @{$ret{$data[0]}}, $data[1];

        return 1;
    };

    FileUtils::file_foreach_line($file, $callback);

    return \%ret;
}

sub _opatch_installed_products_9_2 {

    FileUtils::fatal("implement me!");
}

1;

__END__
