# %%
from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName('spark-nltk')
sc = SparkContext(conf=conf)
import nltk
# %%
data = sc.textFile('Nixon.txt')
# %%
import nltk
# %%
def word_tokenize(x):
    # import nltk
    lowerW = x.lower()
    return nltk.word_tokenize(x)

# %%
words = data.flatMap(word_tokenize)
# %%
words.collect()
# %%
from nltk.corpus import stopwords
stop_words = set(stopwords.words('english'))
stopW = words.filter(lambda word: word[0] not in stop_words and word[0] != '')
stopW.collect()
 # %%
############################### علایم 
# remove punctuation
import string
list_punc = list(string.punctuation)
filtered_data = stopW.filter(lambda punc: punc not in list_punc)
filtered_data.collect()
############################### شمارش

# %%
filtered_data.distinct().collect()
# %%
filtered_data.count()
# %%
stopW.count()
# %%
words.count()
# %%
################################ دستورات کلمات
nltk.download('averaged_perceptron_tagger')
# %%
def pos_tags(x):
    return nltk.pos_tag([x])

pos_word = filtered_data.map(pos_tags)

pos_word.collect()
# %%
################################ریشه یابی 
nltk.download('wordnet')
# %%
from nltk.stem import WordNetLemmatizer
def lema(x):
    lema = WordNetLemmatizer()
    return lema.lemmatize(x)
#################################
# %%
lem_words=filtered_data.map(lema)
lem_words.collect()

# %%