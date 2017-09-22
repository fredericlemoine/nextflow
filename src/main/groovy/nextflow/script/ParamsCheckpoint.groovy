/*
 * Copyright (c) 2013-2017, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2017, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.script
import java.nio.file.Path

import groovy.transform.InheritConstructors
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.exception.IllegalFileException
import nextflow.file.FilePatternSplitter
import nextflow.processor.ProcessConfig
import nextflow.util.BlankSeparatedList
/**
 * Model a process generic checkpoint parameter
 *
 *  @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

interface CheckpointParam {

    /**
     * @return The parameter name getter
     */
    String getName()

    short getIndex()
}

/**
 * Model a process *file* output parameter
 */
@Slf4j
@InheritConstructors
class FileCheckpointParam extends BaseOutParam implements OutParam, OptionalParam {
    /**
     * The character used to separate multiple names (pattern) in the output specification
     */
    protected String separatorChar = ':'

    /**
     * When {@code true} star wildcard (*) matches hidden files (files starting with a dot char)
     * By default it does not, coherently with linux bash rule
     */
    protected boolean includeHidden

    /**
     * The type of path to output, either {@code file}, {@code dir} or {@code any}
     */
    protected String type

    /**
     * Maximum number of directory levels to visit (default: no limit)
     */
    protected Integer maxDepth

    /**
     * When true it follows symbolic links during directories tree traversal, otherwise they are managed as files (default: true)
     */
    protected boolean followLinks = true

    protected boolean glob = true

    private GString gstring

    private Closure<String> dynamicObj

    private String filePattern

    String getSeparatorChar() { separatorChar }

    boolean getHidden() { includeHidden }

    boolean getIncludeInputs() { includeInputs }

    String getType() { type }

    Integer getMaxDepth() { maxDepth }

    boolean getFollowLinks() { followLinks }

    boolean getGlob() { glob }


    /**
     * @return {@code true} when the file name is parametric i.e contains a variable name to be resolved, {@code false} otherwise
     */
    boolean isDynamic() { dynamicObj || gstring != null }

    FileCheckpointParam separatorChar( String value ) {
        this.separatorChar = value
        return this
    }

    FileCheckpointParam includeInputs( boolean flag ) {
        this.includeInputs = flag
        return this
    }

    FileCheckpointParam includeHidden( boolean flag ) {
        this.includeHidden = flag
        return this
    }

    FileCheckpointParam hidden( boolean flag ) {
        this.includeHidden = flag
        return this
    }

    FileCheckpointParam type( String value ) {
        assert value in ['file','dir','any']
        type = value
        return this
    }

    FileCheckpointParam maxDepth( int value ) {
        maxDepth = value
        return this
    }

    FileCheckpointParam followLinks( boolean value ) {
        followLinks = value
        return this
    }

    FileCheckpointParam glob( boolean value ) {
        glob = value
        return this
    }

    BaseOutParam bind( obj ) {

        if( obj instanceof GString ) {
            gstring = obj
            return this
        }

        if( obj instanceof TokenVar ) {
            this.nameObj = obj.name
            dynamicObj = { delegate.containsKey(obj.name) ? delegate.get(obj.name): obj.name }
            return this
        }

        if( obj instanceof Closure ) {
            dynamicObj = obj
            return this
        }

        this.filePattern = obj.toString()
        return this
    }

    List<String> getFilePatterns(Map context, Path workDir) {

        def entry = null
        if( dynamicObj ) {
            entry = context.with(dynamicObj)
        }
        else if( gstring != null ) {
            def strict = (getName() == null)
            try {
                entry = gstring.cloneWith(context)
            }
            catch( MissingPropertyException e ) {
                if( strict )
                    throw e
            }
        }
        else {
            entry = filePattern
        }

        if( !entry )
            return []

        if( entry instanceof Path )
            return [ relativize(entry, workDir) ]

        // handle a collection of files
        if( entry instanceof BlankSeparatedList || entry instanceof List ) {
            return entry.collect { relativize(it.toString(), workDir) }
        }

        // normalize to a string object
        final nameString = entry.toString()
        if( separatorChar && nameString.contains(separatorChar) ) {
            return nameString.split(/\${separatorChar}/).collect { String it-> relativize(it, workDir) }
        }

        return [relativize(nameString, workDir)]

    }

    @PackageScope String getFilePattern() { filePattern }

    @PackageScope
    static String clean(String path) {
        while (path.startsWith('/') ) {
            path = path.substring(1)
        }
        return path
    }

    @PackageScope
    String relativize(String path, Path workDir) {
        if( !path.startsWith('/') )
            return path

        final dir = workDir.toString()
        if( !path.startsWith(dir) )
            throw new IllegalFileException("File `$path` is out of the scope of process working dir: $workDir")

        if( path.length()-dir.length()<2 )
            throw new IllegalFileException("Missing output file name")

        return path.substring(dir.size()+1)
    }

    @PackageScope
    String relativize(Path path, Path workDir) {
        if( !path.isAbsolute() )
            return glob ? FilePatternSplitter.GLOB.escape(path) : path

        if( !path.startsWith(workDir) )
            throw new IllegalFileException("File `$path` is out of the scope of process working dir: $workDir")

        if( path.nameCount == workDir.nameCount )
            throw new IllegalFileException("Missing output file name")

        final rel = path.subpath(workDir.getNameCount(), path.getNameCount())
        return glob ? FilePatternSplitter.GLOB.escape(rel) : rel
    }

    /**
     * Override the default to allow null as a value name
     * @return
     */
    String getName() {
        return nameObj ? super.getName() : null
    }

}

final class DefaultCheckpointParam extends BaseOutParam {

    DefaultCheckpointParam( ProcessConfig config ) {
        super(config)
        bind('-')
    }
}

/**
 * Container to hold all process outputs
 */
class CheckpointsList implements List<CheckpointParam> {

    @Delegate
    private List<CheckpointParam> target = new LinkedList<>()

    List<String> getNames() { target *. name }

    def <T extends CheckpointParam> List<T> ofType( Class<T>... classes ) {
        (List<T>) target.findAll { it.class in classes }
    }

    void setSingleton( boolean value ) {
        target.each { BaseOutParam param -> param.singleton = value }
    }
}
