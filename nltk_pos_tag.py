import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

TEST = True
VISUALIZE = False

def processContent(text: str, filter = False) -> list:
    try:
        tokenized = nltk.word_tokenize(text)
        tagged = nltk.pos_tag(tokenized)

        # For testing and visualization
        border = "*"*42
        if filter:
            filter_tagged = []
            for element in tagged:
                for pos in ["NN","NNS","NNP","NNPS","JJ","VBG"]:
                    if pos in element:
                        filter_tagged.append(element)
            return filter_tagged # this is a list of strings

        if VISUALIZE:
            print(border)
            chunkGram = r"""Chunk: {<RB.?>*<VB.?>*<NNP>}"""
            chunkParser = nltk.RegexpParser(chunkGram)

            chunked = chunkParser.parse(tagged)
            print("chunked = {}".format(chunked))
            chunked.draw()
            print(border)

        return tagged # this is a list of tuples

    except Exception as e:
        return([e]) # this is a list with one exception object

"""
POS tag list:

CC	coordinating conjunction
CD	cardinal digit
DT	determiner
EX	existential there (like: "there is" ... think of it like "there exists")
FW	foreign word
IN	preposition/subordinating conjunction
JJ	adjective	'big'
JJR	adjective, comparative	'bigger'
JJS	adjective, superlative	'biggest'
LS	list marker	1)
MD	modal	could, will
NN	noun, singular 'desk'
NNS	noun plural	'desks'
NNP	proper noun, singular	'Harrison'
NNPS	proper noun, plural	'Americans'
PDT	predeterminer	'all the kids'
POS	possessive ending	parent's
PRP	personal pronoun	I, he, she
PRP$	possessive pronoun	my, his, hers
RB	adverb	very, silently,
RBR	adverb, comparative	better
RBS	adverb, superlative	best
RP	particle	give up
TO	to	go 'to' the store.
UH	interjection	errrrrrrrm
VB	verb, base form	take
VBD	verb, past tense	took
VBG	verb, gerund/present participle	taking
VBN	verb, past participle	taken
VBP	verb, sing. present, non-3d	take
VBZ	verb, 3rd person sing. present	takes
WDT	wh-determiner	which
WP	wh-pronoun	who, what
WP$	possessive wh-pronoun	whose
WRB	wh-abverb	where, when
"""