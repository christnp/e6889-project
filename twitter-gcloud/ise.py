#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
"""Infromation extraction using iterative set expansion

CS6111: Project 2, Group 33
Rashad Barghouti, UNI:rb3074
"""

# System imports
import os
import sys
import time
import logging
import argparse
import subprocess
from pathlib import Path
from textwrap import wrap
from xml.dom import minidom
import xml.etree.ElementTree as ET

# External imports
import tika
from googleapiclient.discovery import build

# CoreNLP classes
class Token:
    pass
class Entity:
    pass
class Relation:
    pass

# Data structure for program-wide data
class ISE:
    pass

# Set up simple logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('ise.log', mode='w')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
formatter = logging.Formatter('%(levelname)s:%(funcName)s: %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

#——————————————————————————————————————————————————————————————————————————————
def _main():
    """Program entry point."""

    # Set up runtime environment
    ise = ISE()
    init_prog(ise)

    X = ise.X
    extracted = 0       # number of extracted tuples from current website
    new_total = 0       # total number of tuples extracted
    current_total = 0   # used to tracks # tuples changing from iter to next
    unchanged = 0       # number of iterations without change in # tuples

    # Start Google custom search service
    service = build("customsearch", "v1", developerKey=ise.api_key)
    search_enabled = True

    # Current query is seed query
    q = ise.q
    qlist = []
    processed_urls = []
    iteration = 1
    while True:

        total_time = time.perf_counter()

        s = ' Iteration: {} — Query: {} '.format(iteration, q)
        print('\n{:=<79}\n{:^79}\n{:=<79}'.format('', s, ''))
        logger.info('\n{:=^79}'.format(s))

        if search_enabled:
            q_results = service.cse().list(q=q, cx=ise.cse_id).execute()
            search_enabled = False

        for item in q_results['items']:

            # Skip this website if processed already
            ise.url = item['link']
            if ise.url in processed_urls:
                continue
            processed_urls.append(ise.url)

            s = 'Processing: {}'.format(ise.url)
            print('\n{}'.format(s))
            logger.info(s)
            website_time = time.perf_counter()

            # Run tika-app. If nonzero returncode, go to next website
            bad_exit_status = run_tika(ise)
            if bad_exit_status:
                continue

            # Run coreNLP's first pipeline
            launch_coreNLP_pipeline(ise, 1)

            # Extract candidate sentences for 2nd pipeline
            prune_sentences(ise.r)

            # Run coreNLP's second pipeline
            launch_coreNLP_pipeline(ise, 2)

            # Parse XML output to get extracted relations (if any)
            extracted = parse_xml_output(ise)
            new_total += extracted
            website_time = tmstr(time.perf_counter() - website_time)
            s = ['Relations extracted from this website: {}'.format(extracted)]
            s += ['(Overall: {})'.format(new_total)]
            s = ' '.join(s + ['— time: {}'.format(website_time)])
            print(s)
            logger.info(s)

        #** Iteration done **#
        logger.info('Iteration {} completed'.format(iteration))

        # Prune sentences below threshold
        done = sort_and_prune_tuples(ise)
        if done:
            total_time = tmstr(time.perf_counter()-total_time)
            s = ['Program reached number of tuples: {}'.format(ise.k)]
            s += ['(total time: {}). Shutting down...'.format(total_time)]
            s = ' '.join(s)
            print(s)
            logger.info(s)
            sys.exit(0)

        # Check if progress is being made from iteration to iteration
        if new_total > current_total:
            current_total = new_total
        else:
            # Exit if # of tuples has not changed in 4 consecutive iterations
            unchanged += 1
            if unchanged == 4:
                s = ('Program unable to extract needed number of tuples. '
                        'Shutting down.')
                print(s, file=sys.stderr)
                logger.info(s)
                sys.exit(0)

        # Select query tuple for next iteration. X has been sorted already in
        # descending order
        found = False
        q = ise.q.lower()
        for x in X:
            x0, x1 = x.entities[0].value, x.entities[1].value
            # First check if x is in original (seed) query, which is a string
            if x0.lower in q and x1.lower in q:
                continue
            for q0, q1 in qlist:
                if x0.lower() == q0.lower() and x1.lower() == q1.lower():
                    break
            else:
                found = True
                break
        if found:
            qlist.append((x0, x1))
            q = ' '.join((x0, x1))
            logger.info('New query: %s', q)
        else:
            s = ('Cannot generate more queries from extracted tuple list. '
                    'Terminating program.')
            print(s)
            logger.info(s)

        # Terminate if after 5 iterations to avoid running CSE endlessly
        iteration += 1
        if iteration == 6:
            s = ('Program unable to extract needed number of tuples. '
                    'Shutting down.')
            print(s, file=sys.stderr)
            logger.info(s)
            sys.exit(0)

        # Do next iteration
        search_enabled = True

#——————————————————————————————————————————————————————————————————————————————
def sort_and_prune_tuples(ise):
    """Prune tuples below threshold and print output messages if done.

    Arguments:
        ise — system data structure
    Output:
        done — boolean indicating whether top-k results have been achieved
    """

    # Sort X; (no need to convert probabilities to floats; strings will do)
    ise.X.sort(key=lambda x: x.probability, reverse=True)

    logger.info('current top-k value: %s', ise.top_k)
    thresh = str(ise.t)
    for x in ise.X:
        if x.probability > thresh:
            logger.info('confidence: %s, thresh: %s', x.probability, thresh)
            ise.top_k += 1
    logger.info('updated top-k value: %s', ise.top_k)

    #if ise.top_k >= ise.k:
    if ise.top_k:
        print('Pruning relations below threshold')
        print('Number of tuples after pruning: {}'.format(ise.top_k))
        print('{:=^79}'.format(' ALL RELATIONS '))
        for n in range(ise.top_k):
            s = ['RelationType: {} |'.format(ise.r_type)]
            s += ['Confidence: {:.3f} |'.format(float(ise.X[n].probability))]
            s += ['Entity #1: {}'.format(ise.X[n].entities[0].value)]
            s += ['({})\t'.format(ise.X[n].entities[0].type)]
            s += ['Entity #2: {}'.format(ise.X[n].entities[1].value)]
            s += ['({})'.format(ise.X[n].entities[1].type)]
            print(' '.join(s))

    retval = False
    if ise.top_k >= ise.k:
        retval = True

    return retval
#——————————————————————————————————————————————————————————————————————————————
def parse_xml_output(ise):
    """Parse the second pipeline's XML output to get extracted relations.

    Arguments:
        ise — system data structure
    Returns:
        num_relations — number of relations extracted from this website

    This routine updates the list ise.X with new, non-duplicate tuples
    """

    rtype = ise.r_type
    entity_type = {'Live_In': ['PEOPLE', 'LOCATION'],
                   'Located_In':['LOCATION', 'LOCATION'],
                   'OrgBased_In': ['ORGANIZATION', 'LOCATION'],
                   'Work_For': ['ORGANIZATION', 'PEOPLE']}

    sentences = ET.parse('input.txt.xml').getroot()[0][0]
    logger.info('processing pipeline-2 output (%s sentences)', len(sentences))
    relations = []
    for sentence in sentences:
        logger.info('sentence #%s', sentence.attrib['id'])
        valid_relations = []
        entities = []
        tokens = []
        words = []

        # If no relations were extracted, skip sentence
        tree = sentence.find('MachineReading').find('relations')
        if tree is None:
            logger.info('no relations were extracted from this sentence')
            continue

        # Consider only relations of type 'r'
        for rel in tree:
            rel_valid = False
            if rel.text.rstrip() == rtype:
                e0 = rel[0][0]
                e1 = rel[0][1]
                if ((e0.text.rstrip() == entity_type[rtype][0] and
                     e1.text.rstrip() == entity_type[rtype][1]) or
                    (e0.text.rstrip() == entity_type[rtype][1] and
                     e1.text.rstrip() == entity_type[rtype][0])):
                        rel_valid = True
            if rel_valid:
                valid_relations.append(rel)

        # If no relations, go to next sentence
        if not valid_relations:
            logger.info('no relations of type "%s" found', rtype)
            continue

        # Create tokens list
        for t in sentence.iter('token'):
            token = Token()
            token.id = t.attrib['id']
            token.word = t.find('word').text
            tokens.append(token)
            words.append(token.word)

        sentence_string = ' '.join(words)

        # Get entities
        ents = sentence.find('MachineReading').find('entities')
        for ent in ents:
            entity = Entity()
            entity.type = ent.text.rstrip()
            # Do not add entity if its type does not fit with relation. (There
            # were cases in which entity_type = 'O' was attached to relation
            # 'Work_For')
            if entity.type not in entity_type[rtype]:
                continue
            entity.id = ent.attrib['id']
            entity_token_id = ent.find('span').attrib['end']
            for token in tokens:
                if entity_token_id == token.id:
                    entity.value = token.word
                    break
            entities.append(entity)

        # Get relations' data
        for rel in valid_relations:
            e0 = rel[0][0]
            e1 = rel[0][1]
            if e0.text.rstrip() == entity_type[rtype][1]:
                e0 = rel[0][1]
                e1 = rel[0][0]
            for entity in entities:
                if e0.attrib['id'] == entity.id:
                    e0 = entity
                    break
            for entity in entities:
                if e1.attrib['id'] == entity.id:
                    e1 = entity
                    break
            # Get confidence value
            for p in rel.iter('probability'):
                if p.find('label').text == rtype:
                    prob = p.find('value').text
                    break
            # If this is a duplicate with lower confidence value, don't add it
            do_append = True
            for relation in relations:
                if (relation.entities[0].id == e0.id and
                        relation.entities[1].id == e1.id):
                    do_append = False
                    if prob <= relation.probability:
                        s = ['relation is a lower-confidence duplicate;']
                        logger.info(' '.join(s + ['discarding']))
                        break
                    else:
                        s = 'relation is a higher-confidence duplicate; will'
                        logger.info(s)
                        logger.info('use to replace previously extracted one.')
                        # replace this relation
                        relation.entities[0] = e0
                        relation.entities[1] = e1
                        relation.probability = prob

            # Add relation to list
            if do_append:
                relation = Relation()
                relation.entities = []
                relation.entities.append(e0)
                relation.entities.append(e1)
                relation.probability = prob
                relations.append(relation)

                # TODO: uncomment this block and delete the one after it before
                # submitting; it matches the output format of the reference
                # implementation.
                #
                #print('{:=^79}'.format(' EXTRACTED RELATION '))
                #print('Sentence: {}'.format(sentence_string))
                #print('{:<79}'.format(sentence_string))
                #s = ['RelationType: {} |'.format(rtype)]
                #s += ['Confidence: {} |'.format(relation.probability)]
                #s += ['EntityType1: {} |'.format(relation.entities[0].type)]
                #s += ['EntityValue1: {} |'.format(relation.entities[0].value)]
                #s += ['EntityType2: {} |'.format(relation.entities[1].type)]
                #s += ['EntityValue2: {}'.format(relation.entities[1].value)]
                #print(' '.join(s))
                #print('{:=^79}'.format(' END OF RELATION DESC '))
                s = ['{:=^79}'.format(' EXTRACTED RELATION ')]
                s1 = wrap('Sentence: {}'.format(sentence_string), width=79)
                s += ['\n'.join(s1)]
                s += ['{:<13} {:<12}  |  {:<13} {}'.format('RelationType:',
                        rtype, 'Confidence:', relation.probability)]
                s += ['{:<13} {:<12}  |  {:<13} {}'.format(
                        'EntityType1:', relation.entities[0].type,
                        'EntityValue1:', relation.entities[0].value)]
                s += ['{:<13} {:<12}  |  {:<13} {}'.format(
                        'EntityType2:', relation.entities[1].type,
                        'EntityValue2:', relation.entities[1].value)]
                s += ['{:=^79}'.format(' END OF RELATION DESC ')]
                s = '\n'.join(s)
                print(s)
                logger.info('appending relation\n%s', s)

    # Add nonduplicates extracted from this website to X
    logger.info('adding relations extracted from this website to X')
    for relation in relations:
        do_append = True
        for x in ise.X:
            if (relation.entities[0].value == x.entities[0].value and
                    relation.entities[1].value == x.entities[1].value):
                do_append = False
                if relation.probability <= x.probability:
                    s = 'discarding relation: duplicate in X has higher prob.'
                    logger.info(s)
                    break
                else:
                    logger.info('replacing lower-confidence duplicate in X')
                    # Replace existing x with this new relation
                    x.entities[0] = relation.entities[0]
                    x.entities[1] = relation.entities[1]
                    x.probability = relation.probability

        # Add relation to list
        if do_append:
            ise.X.append(relation)

    return(len(relations))

#——————————————————————————————————————————————————————————————————————————————
def launch_coreNLP_pipeline(ise, pipeline):
    """Launch CoreNLP process.

    Arguments:
        ise — ISE struct with subprocess command args initialized
        pipeline — integer value of 1 or 2 designating pipeline to run
    Returns:
        input.txt.xml — XML output file
    """

    if pipeline == 1:
        props_file = ise.p1_props_file
        logger.info("running CoreNLP's first pipeline")
    else:
        props_file = ise.p2_props_file
        logger.info("running CoreNLP's second pipeline")

    # Launching pipeline
    #
    cmd = ise.coreNLPcmd + ['-props', props_file]
    try:
        completed_proc = subprocess.run(cmd, **ise.kwargs)
    except subprocess.CalledProcessError as e:
        logger.error('CoreNLP process exited with nonzero returncode')
        if e.stderr is not None:
            print(e.stderr)
        sys.exit(1)

#——————————————————————————————————————————————————————————————————————————————
def run_tika(ise):
    """Run tika-app to extract plain text from website.

    Arguments:
        ise — ise data structure with initialized for values for tika-app
              pathname and the website URL
    Returns:
        returncode: 0=success, nonzero=all sorts of things

    Other:
        If the tika-app process call completes successfully, the extracted
        text is written to file 'input.txt' in local directory.
    """

    logger.info('extracting plain text from website.')
    cmd = ['java', '-jar', ise.tika_app, '-T', ise.url]
    # use separate pipes for stdout and stderr
    kwargs = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE,
              'universal_newlines': True, 'check': True}
    try:
        completed_proc = subprocess.run(cmd, **kwargs)
    except subprocess.CalledProcessError as e:
        if e.returncode != 0:
            logger.error('tika-app process terminated with nonzero status.')
        if e.stderr is not None:
            print(e.stderr)
        s = ('Program could not extract text content from this website; '
                'moving to the next one...')
        print(s, file=sys.stderr)
        logger.info(s)
        return 1

    # Write extracted text to input.txt
    with open('input.txt', 'w') as f:
        print('{}'.format(completed_proc.stdout), file=f)

    return 0

#——————————————————————————————————————————————————————————————————————————————
def prune_sentences(r):
    """Extract sentences with relevant named entities.

    Input:
        r — relation type id
    Output:
        input.txt — file to which extracted sentences are written
    """

    logger.info('removing sentences with nonrelevant named entities.')

    text = []
    LIVE_IN, LOCATED_IN, ORG_BASED_IN, WORK_FOR = 1, 2, 3, 4
    sentences = ET.parse('input.txt.xml').getroot()[0][0]
    for sentence in sentences:
        e = dict(PERSON=0, ORGANIZATION=0, LOCATION=0)
        for ner in sentence.iter('NER'):
            if ner.text in e:
                e[ner.text] += 1

        # If entity counts are present, add sentence string to text list
        if ( (r == LIVE_IN and e['PERSON'] and e['LOCATION']) or
             (r == LOCATED_IN and e['LOCATION'] > 1) or
             (r == ORG_BASED_IN and e['ORGANIZATION'] and e['LOCATION']) or
             (r == WORK_FOR and e['ORGANIZATION'] and e['PERSON'])):
                #text += [' '.join([w.text for w in sentence.iter('word')])]
                s = ' '.join([w.text for w in sentence.iter('word')])
                text.append(s)

    with open('input.txt', 'w') as f:
        print('\n'.join(text), file=f)

#——————————————————————————————————————————————————————————————————————————————
def init_prog(ise):
    """Check for needed runtime apps & set up program execution parameters.

    Arguments:
        ise — empty ISE data structure
    Returns:
        ise.tika_app — pathname of tika-app jar file
    """

    # Check if python3 version is 3.5 or higher
    major, minor, micro, _, _ = sys.version_info
    ver = '{}.{}.{}'.format(major, minor, micro)
    if not (major > 2 and minor > 4):
        s = ['Python v3.5+ is required. Installed version is', ver]
        s += ['See README for more information.']
        logger.error(' '.join(s))
        sys.exit(1)
    logger.info('found Python v3.5+ ({})'.format(ver))

    # Check if JAVA is installed. (For some reason, 'java -version' sends info
    # to stderr, so capture stderr to stdout in same pipe
    kwargs = {'stdout': subprocess.PIPE, 'stderr': subprocess.STDOUT,
              'universal_newlines': True, 'check': True}
    try:
        completed_proc = subprocess.run(['java', '-version'], **kwargs)
    except subprocess.CalledProcessError as e:
        logger.error('command `%s` did not complete successfully.', e.cmd)
        if e.stderr is not None:
            print(e.stderr)
        sys.exit(1)

    java = True if 'Runtime Environment' in completed_proc.stdout else False
    if not java:
        logger.error('no Java installation found on the system.')
        sys.exit(1)
    logger.info('found JRE installation')

    # Get tika-app's pathname
    path = [Path(p) for p in os.environ['PATH'].split(':') if Path(p).is_dir()]
    tika_app = os.getenv('TIKA_APP')
    if tika_app is not None:
        if not Path(tika_app).is_file():
            s = ('pathname defined in TIKA_APP environment variable is '
                    'invalid.')
            logger.error(s)
            tika_app = None
        else:
            s = ('found valid tika-app pathname in TIKA_APP environment '
                    'variable')
            logger.info(s)
            logger.info('tika-app pathname: %s', tika_app)

    if tika_app is None:
        jars = []
        for p in path:
            j = sorted(p.glob('tika-app*.jar'))
            if j:
                # If multiple jars, take highest version
                tika_app = str(j[-1].resolve())
                s = ('found valid tika-app pathname in PATH environment '
                        'variable')
                logger.info(s)
                logger.info('tika-app pathname: %s', tika_app)
                break
    # If still no jar, display error msg and exit
    if tika_app is None:
        s = ["Can't find tika-app jar file. Please see README for info on"]
        s += ['setting its path or installing it.']
        logger.error('\n'.join(s))
        sys.exit(1)

    # Get path to coreNLP.
    cnlp_path = os.getenv('STANFORD_CORENLP_PATH')
    if cnlp_path is not None:
        if not Path(cnlp_path).is_dir():
            logger.error('STANFORD_CORENLP_PATH has invalid path value')
            cnlp_path = None
        else:
            s = ('found CoreNLP path in STANFORD_CORENLP_PATH environment '
                    'variable')
            logger.info(s)
            logger.info('CoreNLP path: %s', cnlp_path)
    # If not found in STANFORD_CORENLP_PATH, try CLASSPATH
    if cnlp_path is None:
        classpath = os.getenv('CLASSPATH')
        if classpath is not None:
            for cp in classpath.split(':'):
                p = Path(cp).parent
                if p.is_dir() and 'stanford-corenlp' in str(p):
                    cnlp_path = str(p)
                    s = ('found CoreNLP path in CLASSPATH environment '
                            'variable')
                    logger.info(s)
                    logger.info('CoreNLP path: %s', cnlp_path)
                    break
    # Try PATH (NOTE: don't next the following if statement with above)
    if cnlp_path is None:
        for p in path:
            # check if valid directory
            if p.is_dir() and 'stanford-corenlp' in str(p):
                cnlp_path = str(p)
                s = ('found valid CoreNLP path in PATH environment variable')
                logger.info(s)
                logger.info('CoreNLP path: %s', cnlp_path)
                break

    # If still no path, display error msg and exit
    if cnlp_path is None:
        s = ("Can't find a CoreNLP installation on the system. Please see\n"
                "README for info on setting one up.")
        logger.error(s)
        sys.exit(1)

    cnlp_path = '{}/*'.format(cnlp_path)

    # Parse cmdline and display startup message
    parse_cmdline(ise)
    s = ['\nProgram Parameters:']
    s += ['Client key  = {}'.format(ise.api_key)]
    s += ['Engine key  = {}'.format(ise.cse_id)]
    s += ['Relation    = {}'.format(ise.r)]
    s += ['Threshold   = {}'.format(ise.t)]
    s += ['Query       = {}'.format(ise.q)]
    s += ['# of Tuples = {}'.format(ise.k)]
    s = '\n'.join(s)
    print(s)
    logger.info('command-line parsed%s\n%s', s, '{:=<79}'.format(''))

    ise.X = []
    ise.r_type = ('Live_In', 'Located_In', 'OrgBased_In', 'Work_For')[ise.r-1]
    ise.tika_app = tika_app
    ise.top_k = 0
    ise.coreNLPcmd = ['java', '-cp', cnlp_path]
    ise.coreNLPcmd += ['-Xmx4g', 'edu.stanford.nlp.pipeline.StanfordCoreNLP']
    ise.kwargs = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE,
                  'universal_newlines': True, 'check': True}
    ise.p1_annotators = 'tokenize, ssplit, pos, lemma, ner'
    ise.p2_annotators = 'tokenize, ssplit, pos, lemma, ner, parse, relation'
    ise.parse_model = 'edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz'
    ise.ner_useSUTime = '0'
    ise.p1_props_file = 'p1.properties'
    ise.p2_props_file = 'p2.properties'
    with open(ise.p1_props_file, 'w') as f:
        print('annotators = {}'.format(ise.p1_annotators), file=f)
        print('file = {}'.format('input.txt'), file=f)
    with open(ise.p2_props_file, 'w') as f:
        print('annotators = {}'.format(ise.p2_annotators), file=f)
        print('parse.model = {}'.format(ise.parse_model), file=f)
        print('ner.useSUTime = {}'.format(ise.ner_useSUTime), file=f)
        print('file = {}'.format('input.txt'), file=f)


#——————————————————————————————————————————————————————————————————————————————
def parse_cmdline(ise):
    """Parse command-line.

    This function parses the command-line arguments and performs simple
    type-checking and validation on the r, t, and k input values

    Arguments:
        ise — system struct object

    Returns:
        The following ise attributes are init:ialized
        api_key — Google's custome search engine's (CSE) API key
        cse_id  — CSE id
             r  — relation type of tuples to be extracted
             q  — seed query
             t  — confidence threshold
             k  — number of tuples to extract
    """

    # Parse command-line
    usage = ['\n\t%(prog)s [-h|--help]', '\n\t%(prog)s @keys <r> <t> <q> <k>']
    usage += ['\n\t%(prog)s <api-key> <cse-id> <r> <t> <q> <k>']
    usage += ['\n\nARGUMENTS:\n']
    usage += ['\t-h|--help print this message and exit\n']
    usage += ["\t keys     the 'keys' file in project's directory. It\n"]
    usage += ["\t          contains the API key and CSE Id\n"]
    usage += ["\t apikey   Google custom search engine's API key\n"]
    usage += ["\t cseid    Google custom search engine's ID\n"]
    usage += ['\t r        integer id of relation to extract\n']
    usage += ['\t t        extraction confidence threshold\n']
    usage += ['\t q        seed query\n']
    usage += ['\t k        requested number of output tuples\n']
    usage = ' '.join(usage)

    parser = argparse.ArgumentParser(usage=usage, add_help=False,
            fromfile_prefix_chars='@')
    #parser = parser.add_argument_group('ARGUMENTS')
    parser.add_argument('api_key', nargs=1, metavar='api-key',
            #help="Google custom search engine's API key"
            help=argparse.SUPPRESS)
    parser.add_argument('cse_id', nargs=1, metavar='cse-id',
            #help="Google custom search engine's ID")
            help=argparse.SUPPRESS)
    parser.add_argument('r', nargs=1, metavar='r', type=int,
            choices=range(1, 5), #help='integer id of relation to extract')
            help=argparse.SUPPRESS)
    parser.add_argument('t', nargs=1, metavar='t', type=float,
            #help='extraction confidence threshold')
            help=argparse.SUPPRESS)
    parser.add_argument('q', nargs=1, metavar='q', #help='seed query')
            help=argparse.SUPPRESS)
    parser.add_argument('k', nargs=1, metavar='k', type=int,
            #help='requested number of output tuples')
            help=argparse.SUPPRESS)
    parser.add_argument('-h', '--help', action='help', help=argparse.SUPPRESS)
    args = parser.parse_args()

    ise.api_key = args.api_key[0]
    ise.cse_id = args.cse_id[0]
    ise.r = args.r[0]
    ise.q = args.q[0]

    # datatype checking is done in parser; check values here
    ise.t = args.t[0]
    if not 0.0 < ise.t <= 1.0:
        logger.error('argument t: invalid value: %s (choose 0.0 < t <= 1.0)',
                ise.t)
        sys.exit(0)
    ise.k = args.k[0]
    if not ise.k > 0:
        logger.error('argument k: invalid value: %s (must be > 0)', ise.k)
        sys.exit(0)

#——————————————————————————————————————————————————————————————————————————————
def tmstr(tm):
    """Format input time in seconds as minute:seconds string."""

    tmstr = []

    t = tm/60.0/60.0
    hrs = int(t)
    if hrs:
        tmstr.append('{:d} hrs '.format(hrs))

    t = (t-hrs)*60.0
    mins = int(t)
    if mins:
        tmstr.append('{:d} min '.format(mins))

    secs = (t-mins)*60.0
    #tmstr.append('{:.1g} sec'.format(secs))
    if int(secs*10000):
        tmstr.append('{:.2f} sec'.format(secs))
    else:
        tmstr.append('{:.0e} sec'.format(secs))

    return ''.join(tmstr)

#——————————————————————————————————————————————————————————————————————————————
# If running as script, start at _main()
#——————————————————————————————————————————————————————————————————————————————
if __name__ == '__main__':

    try:
        _main()
    except KeyboardInterrupt:
        logger.info('\nKeyboardInterrupt <Ctrl-C> caught. Stopping program.')
        print('\nKeyboardInterrupt <Ctrl-C> caught. Stopping', file=sys.stderr)
        sys.exit(1)
