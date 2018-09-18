<h4>Ver2.0 with tensorflow-1.6.0</h4>
<h4>本文档为tensorflow.feature_column模块各方法参数说明</h4>
<h4>weilearn 提供了此中函数的反射</h4>


<table border=0 cellpadding=0 cellspacing=0 width=2594 style='border-collapse:
 collapse;table-layout:fixed;width:1945pt'>
 <col class=xl65 width=287 style='mso-width-source:userset;mso-width-alt:9173;
 width:215pt'>
 <col width=260 style='mso-width-source:userset;mso-width-alt:8320;width:195pt'>
 <col width=2047 style='mso-width-source:userset;mso-width-alt:65493;
 width:1535pt'>
 <tr height=21 style='height:16.0pt'>
  <td height=21 class=xl65 width=287 style='height:16.0pt;width:215pt'>operator</td>
  <td width=260 style='width:195pt'>args_key</td>
  <td width=2047 style='width:1535pt'>args_comment</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=2 height=42 class=xl65 style='height:32.0pt'>bucketized_column</td>
  <td>source_column</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A one-dimensional dense
  column which is generated with`numeric_column`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>boundaries</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A sorted list or tuple of
  floats specifying the boundaries.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=3 height=63 class=xl65 style='height:48.0pt'>categorical_column_with_hash_bucket</td>
  <td>key</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A unique string identifying
  the input feature. It is used as thecolumn name and the dictionary key for
  feature parsing configs, feature`Tensor` objects, and feature columns.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>hash_bucket_size</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An int &gt; 1. The number of
  buckets.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>dtype</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>The type of features. Only
  string and integer types are supported.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=3 height=63 class=xl65 style='height:48.0pt'>categorical_column_with_identity</td>
  <td>key</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A unique string identifying
  the input feature. It is used as thecolumn name and the dictionary key for
  feature parsing configs, feature`Tensor` objects, and feature columns.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>num_buckets</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Range of inputs and outputs
  is `[0, num_buckets)`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>default_value</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>If `None`, this column's
  graph operations will fail forout-of-range inputs. Otherwise, this value must
  be in the range`[0, num_buckets)`, and will replace inputs in that range.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=6 height=126 class=xl65 style='height:96.0pt'>categorical_column_with_vocabulary_file</td>
  <td>key</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A unique string identifying
  the input feature. It is used as thecolumn name and the dictionary key for
  feature parsing configs, feature`Tensor` objects, and feature columns.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>vocabulary_file</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>The vocabulary file name.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>vocabulary_size</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Number of the elements in the
  vocabulary. This must be nogreater than length of `vocabulary_file`, if less
  than length, latervalues are ignored. If None, it is set to the length of
  `vocabulary_file`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>num_oov_buckets</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Non-negative integer, the
  number of out-of-vocabularybuckets. All out-of-vocabulary inputs will be
  assigned IDs in the range`[vocabulary_size, vocabulary_size+num_oov_buckets)`
  based on a hash ofthe input value. A positive `num_oov_buckets` can not be
  specified with`default_value`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>default_value</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>The integer ID value to
  return for out-of-vocabulary featurevalues, defaults to `-1`. This can not be
  specified with a positive`num_oov_buckets`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>dtype</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>The type of features. Only
  string and integer types are supported.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=5 height=105 class=xl65 style='height:80.0pt'>categorical_column_with_vocabulary_list</td>
  <td>key</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A unique string identifying
  the input feature. It is used as thecolumn name and the dictionary key for
  feature parsing configs, feature`Tensor` objects, and feature columns.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>vocabulary_list</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An ordered iterable defining
  the vocabulary. Each featureis mapped to the index of its value (if present)
  in `vocabulary_list`.Must be castable to `dtype`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>dtype</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>The type of features. Only
  string and integer types are supported.If `None`, it will be inferred from
  `vocabulary_list`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>default_value</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>The integer ID value to
  return for out-of-vocabulary featurevalues, defaults to `-1`. This can not be
  specified with a positive`num_oov_buckets`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>num_oov_buckets</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Non-negative integer, the
  number of out-of-vocabularybuckets. All out-of-vocabulary inputs will be
  assigned IDs in the range`[len(vocabulary_list),
  len(vocabulary_list)+num_oov_buckets)` based on ahash of the input value. A
  positive `num_oov_buckets` can not be specifiedwith `default_value`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=3 height=63 class=xl65 style='height:48.0pt'>crossed_column</td>
  <td>keys</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An iterable identifying the
  features to be crossed. Each element canbe either:* string: Will use the
  corresponding feature which must be of string type.* `_CategoricalColumn`:
  Will use the transformed tensor produced by thiscolumn. Does not support
  hashed categorical column.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>hash_bucket_size</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An int &gt; 1. The number of
  buckets.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>hash_key</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Specify the hash_key that
  will be used by the `FingerprintCat64`function to combine the crosses
  fingerprints on SparseCrossOp (optional).</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=8 height=168 class=xl65 style='height:128.0pt'>embedding_column</td>
  <td>categorical_column</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A `_CategoricalColumn`
  created by a`categorical_column_with_*` function. This column produces the
  sparse IDsthat are inputs to the embedding lookup.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>dimension</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An integer specifying
  dimension of the embedding, must be &gt; 0.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>combiner</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A string specifying how to
  reduce if there are multiple entriesin a single row. Currently 'mean',
  'sqrtn' and 'sum' are supported, with'mean' the default. 'sqrtn' often
  achieves good accuracy, in particularwith bag-of-words columns. Each of this
  can be thought as example levelnormalizations on the column. For more
  information, see`tf.embedding_lookup_sparse`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>initializer</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A variable initializer
  function to be used in embeddingvariable initialization. If not specified,
  defaults to`tf.truncated_normal_initializer` with mean `0.0` and standard
  deviation`1/sqrt(dimension)`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>ckpt_to_load_from</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>String representing
  checkpoint name/pattern from which torestore column weights. Required if
  `tensor_name_in_ckpt` is not `None`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>tensor_name_in_ckpt</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Name of the `Tensor` in
  `ckpt_to_load_from` fromwhich to restore the column weights. Required if
  `ckpt_to_load_from` isnot `None`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>max_norm</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>If not `None`, embedding
  values are l2-normalized to this value.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>trainable</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Whether or not the embedding
  is trainable. Default is True.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 class=xl65 style='height:16.0pt'>indicator_column</td>
  <td>categorical_column</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A `_CategoricalColumn` which
  is created by`categorical_column_with_*` or `crossed_column` functions.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=5 height=105 class=xl65 style='height:80.0pt'>input_layer</td>
  <td>features</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A mapping from key to
  tensors. `_FeatureColumn`s look up via thesekeys. For example
  `numeric_column('price')` will look at 'price' key inthis dict. Values can be
  a `SparseTensor` or a `Tensor` depends oncorresponding `_FeatureColumn`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>feature_columns</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An iterable containing the
  FeatureColumns to use as inputsto your model. All items should be instances
  of classes derived from`_DenseColumn` such as `numeric_column`,
  `embedding_column`,`bucketized_column`, `indicator_column`. If you have
  categorical features,you can wrap them with an `embedding_column` or
  `indicator_column`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>weight_collections</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A list of collection names to
  which the Variable will beadded. Note that variables will also be added to
  collections`tf.GraphKeys.GLOBAL_VARIABLES` and
  `ops.GraphKeys.MODEL_VARIABLES`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>trainable</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>If `True` also add the
  variable to the graph collection`GraphKeys.TRAINABLE_VARIABLES` (see
  `tf.Variable`).</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>cols_to_vars</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>If not `None`, must be a
  dictionary that will be filled with amapping from `_FeatureColumn` to list of
  `Variable`s.<span style='mso-spacerun:yes'>&nbsp; </span>For example,
  afterthe call, we might have cols_to_vars
  ={_EmbeddingColumn(categorical_column=_HashedCategoricalColumn(key='sparse_feature',
  hash_bucket_size=5, dtype=tf.string),dimension=10): [&lt;tf.Variable
  'some_variable:0' shape=(5, 10),&lt;tf.Variable 'some_variable:1' shape=(5,
  10)]}If a column creates no variables, its value will be an empty list.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=7 height=147 class=xl65 style='height:112.0pt'>linear_model</td>
  <td>features</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A mapping from key to
  tensors. `_FeatureColumn`s look up via thesekeys. For example
  `numeric_column('price')` will look at 'price' key inthis dict. Values are
  `Tensor` or `SparseTensor` depending oncorresponding `_FeatureColumn`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>feature_columns</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An iterable containing the
  FeatureColumns to use as inputsto your model. All items should be instances
  of classes derived from`_FeatureColumn`s.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>units</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An integer, dimensionality of
  the output space. Default value is 1.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>sparse_combiner</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A string specifying how to
  reduce if a sparse column ismultivalent. Currently &quot;mean&quot;,
  &quot;sqrtn&quot; and &quot;sum&quot; are supported, with &quot;sum&quot;the
  default. &quot;sqrtn&quot; often achieves good accuracy, in particular
  withbag-of-words columns. It combines each sparse columns independently.*
  &quot;sum&quot;: do not normalize features in the column* &quot;mean&quot;:
  do l1 normalization on features in the column* &quot;sqrtn&quot;: do l2
  normalization on features in the column</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>weight_collections</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A list of collection names to
  which the Variable will beadded. Note that, variables will also be added to
  collections`tf.GraphKeys.GLOBAL_VARIABLES` and
  `ops.GraphKeys.MODEL_VARIABLES`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>trainable</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>If `True` also add the
  variable to the graph collection`GraphKeys.TRAINABLE_VARIABLES` (see
  `tf.Variable`).</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>cols_to_vars</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>If not `None`, must be a
  dictionary that will be filled with amapping from `_FeatureColumn` to
  associated list of `Variable`s.<span style='mso-spacerun:yes'>&nbsp;
  </span>Forexample, after the call, we might have cols_to_vars =
  {_NumericColumn(key='numeric_feature1', shape=(1,):[&lt;tf.Variable
  'linear_model/price2/weights:0' shape=(1, 1)&gt;],'bias': [&lt;tf.Variable
  'linear_model/bias_weights:0'
  shape=(1,)&gt;],_NumericColumn(key='numeric_feature2', shape=(2,)):[&lt;tf.Variable
  'linear_model/price1/weights:0' shape=(2, 1)&gt;]}If a column creates no
  variables, its value will be an empty list. Notethat cols_to_vars will also
  contain a string key 'bias' that maps to alist of Variables.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 class=xl65 style='height:16.0pt'>make_parse_example_spec</td>
  <td>feature_columns</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An iterable containing all
  feature columns. All itemsshould be instances of classes derived from
  `_FeatureColumn`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=5 height=105 class=xl65 style='height:80.0pt'>numeric_column</td>
  <td>key</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A unique string identifying
  the input feature. It is used as thecolumn name and the dictionary key for
  feature parsing configs, feature`Tensor` objects, and feature columns.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>shape</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An iterable of integers
  specifies the shape of the `Tensor`. Aninteger can be given which means a
  single dimension `Tensor` with givenwidth. The `Tensor` representing the
  column will have the shape of[batch_size] + `shape`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>default_value</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A single value compatible
  with `dtype` or an iterable ofvalues compatible with `dtype` which the column
  takes on during`tf.Example` parsing if data is missing. A default value of
  `None` willcause `tf.parse_example` to fail if an example does not contain
  thiscolumn. If a single value is provided, the same value will be applied
  asthe default value for every item. If an iterable of values is provided,the
  shape of the `default_value` should be equal to the given `shape`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>dtype</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>defines the type of values.
  Default value is `tf.float32`. Must be anon-quantized, real integer or
  floating point type.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>normalizer_fn</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>If not `None`, a function
  that can be used to normalize thevalue of the tensor after `default_value` is
  applied for parsing.Normalizer function takes the input `Tensor` as its
  argument, and returnsthe output `Tensor`. (e.g. lambda x: (x - 3.0) / 4.2).
  Please note thateven though the most common use case of this function is
  normalization, itcan be used for any kind of Tensorflow transformations.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=9 height=189 class=xl65 style='height:144.0pt'>shared_embedding_columns</td>
  <td>categorical_columns</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>List of categorical columns
  created by a`categorical_column_with_*` function. These columns produce the
  sparse IDsthat are inputs to the embedding lookup. All columns must be of the
  sametype and have the same arguments except `key`. E.g. they can
  becategorical_column_with_vocabulary_file with the same vocabulary_file.Some
  or all columns could also be weighted_categorical_column.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>dimension</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>An integer specifying
  dimension of the embedding, must be &gt; 0.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>combiner</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A string specifying how to
  reduce if there are multiple entriesin a single row. Currently 'mean',
  'sqrtn' and 'sum' are supported, with'mean' the default. 'sqrtn' often
  achieves good accuracy, in particularwith bag-of-words columns. Each of this
  can be thought as example levelnormalizations on the column. For more
  information, see`tf.embedding_lookup_sparse`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>initializer</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A variable initializer
  function to be used in embeddingvariable initialization. If not specified,
  defaults to`tf.truncated_normal_initializer` with mean `0.0` and standard
  deviation`1/sqrt(dimension)`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>shared_embedding_collection_name</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Optional name of the
  collection whereshared embedding weights are added. If not given, a
  reasonable name willbe chosen based on the names of `categorical_columns`.
  This is also usedin `variable_scope` when creating shared embedding weights.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>ckpt_to_load_from</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>String representing
  checkpoint name/pattern from which torestore column weights. Required if
  `tensor_name_in_ckpt` is not `None`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>tensor_name_in_ckpt</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Name of the `Tensor` in
  `ckpt_to_load_from` fromwhich to restore the column weights. Required if
  `ckpt_to_load_from` isnot `None`.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>max_norm</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>If not `None`, embedding
  values are l2-normalized to this value.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>trainable</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Whether or not the embedding
  is trainable. Default is True.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td rowspan=3 height=63 class=xl65 style='height:48.0pt'>weighted_categorical_column</td>
  <td>categorical_column</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>A `_CategoricalColumn`
  created by`categorical_column_with_*` functions.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>weight_feature_key</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>String key for weight values.</td>
 </tr>
 <tr height=21 style='height:16.0pt'>
  <td height=21 style='height:16.0pt'>dtype</td>
  <td><span style='mso-spacerun:yes'>&nbsp;</span>Type of weights, such as
  `tf.float32`. Only float and integer weightsare supported.</td>
 </tr>
</table>



<h4>Ver1.0 with tensorflow-1.6.0</h4>
<h4>本文档为tensorflow.feature_column模块各方法参数说明</h4>
<h4>weilearn 提供了此中函数的反射</h4>
<h4>时间原因格式未能充分整理</h4>
<h4>[TODO] 整理成为表格</h4>

```bash
bucketized_column    
      source_column: A one-dimensional dense column which is generated with
        `numeric_column`.
      boundaries: A sorted list or tuple of floats specifying the boundaries.
    
    
categorical_column_with_hash_bucket    
      key: A unique string identifying the input feature. It is used as the
        column name and the dictionary key for feature parsing configs, feature
        `Tensor` objects, and feature columns.
      hash_bucket_size: An int > 1. The number of buckets.
      dtype: The type of features. Only string and integer types are supported.
    
    
categorical_column_with_identity    
      key: A unique string identifying the input feature. It is used as the
        column name and the dictionary key for feature parsing configs, feature
        `Tensor` objects, and feature columns.
      num_buckets: Range of inputs and outputs is `[0, num_buckets)`.
      default_value: If `None`, this column’s graph operations will fail for
        out-of-range inputs. Otherwise, this value must be in the range
        `[0, num_buckets)`, and will replace inputs in that range.
    
    
categorical_column_with_vocabulary_file    
      key: A unique string identifying the input feature. It is used as the
        column name and the dictionary key for feature parsing configs, feature
        `Tensor` objects, and feature columns.
      vocabulary_file: The vocabulary file name.
      vocabulary_size: Number of the elements in the vocabulary. This must be no
        greater than length of `vocabulary_file`, if less than length, later
        values are ignored. If None, it is set to the length of `vocabulary_file`.
      num_oov_buckets: Non-negative integer, the number of out-of-vocabulary
        buckets. All out-of-vocabulary inputs will be assigned IDs in the range
        `[vocabulary_size, vocabulary_size+num_oov_buckets)` based on a hash of
        the input value. A positive `num_oov_buckets` can not be specified with
        `default_value`.
      default_value: The integer ID value to return for out-of-vocabulary feature
        values, defaults to `-1`. This can not be specified with a positive
        `num_oov_buckets`.
      dtype: The type of features. Only string and integer types are supported.
    
    
categorical_column_with_vocabulary_list    
      key: A unique string identifying the input feature. It is used as the
        column name and the dictionary key for feature parsing configs, feature
        `Tensor` objects, and feature columns.
      vocabulary_list: An ordered iterable defining the vocabulary. Each feature
        is mapped to the index of its value (if present) in `vocabulary_list`.
        Must be castable to `dtype`.
      dtype: The type of features. Only string and integer types are supported.
        If `None`, it will be inferred from `vocabulary_list`.
      default_value: The integer ID value to return for out-of-vocabulary feature
        values, defaults to `-1`. This can not be specified with a positive
        `num_oov_buckets`.
      num_oov_buckets: Non-negative integer, the number of out-of-vocabulary
        buckets. All out-of-vocabulary inputs will be assigned IDs in the range
        `[len(vocabulary_list), len(vocabulary_list)+num_oov_buckets)` based on a
        hash of the input value. A positive `num_oov_buckets` can not be specified
        with `default_value`.
    
    
crossed_column    
      keys: An iterable identifying the features to be crossed. Each element can
        be either:
        * string: Will use the corresponding feature which must be of string type.
        * `_CategoricalColumn`: Will use the transformed tensor produced by this
          column. Does not support hashed categorical column.
      hash_bucket_size: An int > 1. The number of buckets.
      hash_key: Specify the hash_key that will be used by the `FingerprintCat64`
        function to combine the crosses fingerprints on SparseCrossOp (optional).
    
    
embedding_column    
      categorical_column: A `_CategoricalColumn` created by a
        `categorical_column_with_*` function. This column produces the sparse IDs
        that are inputs to the embedding lookup.
      dimension: An integer specifying dimension of the embedding, must be > 0.
      combiner: A string specifying how to reduce if there are multiple entries
        in a single row. Currently ’mean’, ’sqrtn’ and ’sum’ are supported, with
        ’mean’ the default. ’sqrtn’ often achieves good accuracy, in particular
        with bag-of-words columns. Each of this can be thought as example level
        normalizations on the column. For more information, see
        `tf.embedding_lookup_sparse`.
      initializer: A variable initializer function to be used in embedding
        variable initialization. If not specified, defaults to
        `tf.truncated_normal_initializer` with mean `0.0` and standard deviation
        `1/sqrt(dimension)`.
      ckpt_to_load_from: String representing checkpoint name/pattern from which to
        restore column weights. Required if `tensor_name_in_ckpt` is not `None`.
      tensor_name_in_ckpt: Name of the `Tensor` in `ckpt_to_load_from` from
        which to restore the column weights. Required if `ckpt_to_load_from` is
        not `None`.
      max_norm: If not `None`, embedding values are l2-normalized to this value.
      trainable: Whether or not the embedding is trainable. Default is True.
    
    
indicator_column    
      categorical_column: A `_CategoricalColumn` which is created by
        `categorical_column_with_*` or `crossed_column` functions.
    
    
input_layer    
      features: A mapping from key to tensors. `_FeatureColumn`s look up via these
        keys. For example `numeric_column(’price’)` will look at ’price’ key in
        this dict. Values can be a `SparseTensor` or a `Tensor` depends on
        corresponding `_FeatureColumn`.
      feature_columns: An iterable containing the FeatureColumns to use as inputs
        to your model. All items should be instances of classes derived from
        `_DenseColumn` such as `numeric_column`, `embedding_column`,
        `bucketized_column`, `indicator_column`. If you have categorical features,
        you can wrap them with an `embedding_column` or `indicator_column`.
      weight_collections: A list of collection names to which the Variable will be
        added. Note that variables will also be added to collections
        `tf.GraphKeys.GLOBAL_VARIABLES` and `ops.GraphKeys.MODEL_VARIABLES`.
      trainable: If `True` also add the variable to the graph collection
        `GraphKeys.TRAINABLE_VARIABLES` (see `tf.Variable`).
      cols_to_vars: If not `None`, must be a dictionary that will be filled with a
        mapping from `_FeatureColumn` to list of `Variable`s.  For example, after
        the call, we might have cols_to_vars =
        {_EmbeddingColumn(
          categorical_column=_HashedCategoricalColumn(
            key=’sparse_feature’, hash_bucket_size=5, dtype=tf.string),
          dimension=10): [<tf.Variable ’some_variable:0’ shape=(5, 10),
                          <tf.Variable ’some_variable:1’ shape=(5, 10)]}
        If a column creates no variables, its value will be an empty list.
    
    
linear_model    
      features: A mapping from key to tensors. `_FeatureColumn`s look up via these
        keys. For example `numeric_column(’price’)` will look at ’price’ key in
        this dict. Values are `Tensor` or `SparseTensor` depending on
        corresponding `_FeatureColumn`.
      feature_columns: An iterable containing the FeatureColumns to use as inputs
        to your model. All items should be instances of classes derived from
        `_FeatureColumn`s.
      units: An integer, dimensionality of the output space. Default value is 1.
      sparse_combiner: A string specifying how to reduce if a sparse column is
        multivalent. Currently "mean", "sqrtn" and "sum" are supported, with "sum"
        the default. "sqrtn" often achieves good accuracy, in particular with
        bag-of-words columns. It combines each sparse columns independently.
          * "sum": do not normalize features in the column
          * "mean": do l1 normalization on features in the column
          * "sqrtn": do l2 normalization on features in the column
      weight_collections: A list of collection names to which the Variable will be
        added. Note that, variables will also be added to collections
        `tf.GraphKeys.GLOBAL_VARIABLES` and `ops.GraphKeys.MODEL_VARIABLES`.
      trainable: If `True` also add the variable to the graph collection
        `GraphKeys.TRAINABLE_VARIABLES` (see `tf.Variable`).
      cols_to_vars: If not `None`, must be a dictionary that will be filled with a
        mapping from `_FeatureColumn` to associated list of `Variable`s.  For
        example, after the call, we might have cols_to_vars = {
          _NumericColumn(
            key=’numeric_feature1’, shape=(1,):
          [<tf.Variable ’linear_model/price2/weights:0’ shape=(1, 1)>],
          ’bias’: [<tf.Variable ’linear_model/bias_weights:0’ shape=(1,)>],
          _NumericColumn(
            key=’numeric_feature2’, shape=(2,)):
          [<tf.Variable ’linear_model/price1/weights:0’ shape=(2, 1)>]}
        If a column creates no variables, its value will be an empty list. Note
        that cols_to_vars will also contain a string key ’bias’ that maps to a
        list of Variables.
    
    
make_parse_example_spec    
      feature_columns: An iterable containing all feature columns. All items
        should be instances of classes derived from `_FeatureColumn`.
    
    
numeric_column    
      key: A unique string identifying the input feature. It is used as the
        column name and the dictionary key for feature parsing configs, feature
        `Tensor` objects, and feature columns.
      shape: An iterable of integers specifies the shape of the `Tensor`. An
        integer can be given which means a single dimension `Tensor` with given
        width. The `Tensor` representing the column will have the shape of
        [batch_size] + `shape`.
      default_value: A single value compatible with `dtype` or an iterable of
        values compatible with `dtype` which the column takes on during
        `tf.Example` parsing if data is missing. A default value of `None` will
        cause `tf.parse_example` to fail if an example does not contain this
        column. If a single value is provided, the same value will be applied as
        the default value for every item. If an iterable of values is provided,
        the shape of the `default_value` should be equal to the given `shape`.
      dtype: defines the type of values. Default value is `tf.float32`. Must be a
        non-quantized, real integer or floating point type.
      normalizer_fn: If not `None`, a function that can be used to normalize the
        value of the tensor after `default_value` is applied for parsing.
        Normalizer function takes the input `Tensor` as its argument, and returns
        the output `Tensor`. (e.g. lambda x: (x - 3.0) / 4.2). Please note that
        even though the most common use case of this function is normalization, it
        can be used for any kind of Tensorflow transformations.
    
    
shared_embedding_columns    
      categorical_columns: List of categorical columns created by a
        `categorical_column_with_*` function. These columns produce the sparse IDs
        that are inputs to the embedding lookup. All columns must be of the same
        type and have the same arguments except `key`. E.g. they can be
        categorical_column_with_vocabulary_file with the same vocabulary_file.
        Some or all columns could also be weighted_categorical_column.
      dimension: An integer specifying dimension of the embedding, must be > 0.
      combiner: A string specifying how to reduce if there are multiple entries
        in a single row. Currently ’mean’, ’sqrtn’ and ’sum’ are supported, with
        ’mean’ the default. ’sqrtn’ often achieves good accuracy, in particular
        with bag-of-words columns. Each of this can be thought as example level
        normalizations on the column. For more information, see
        `tf.embedding_lookup_sparse`.
      initializer: A variable initializer function to be used in embedding
        variable initialization. If not specified, defaults to
        `tf.truncated_normal_initializer` with mean `0.0` and standard deviation
        `1/sqrt(dimension)`.
      shared_embedding_collection_name: Optional name of the collection where
        shared embedding weights are added. If not given, a reasonable name will
        be chosen based on the names of `categorical_columns`. This is also used
        in `variable_scope` when creating shared embedding weights.
      ckpt_to_load_from: String representing checkpoint name/pattern from which to
        restore column weights. Required if `tensor_name_in_ckpt` is not `None`.
      tensor_name_in_ckpt: Name of the `Tensor` in `ckpt_to_load_from` from
        which to restore the column weights. Required if `ckpt_to_load_from` is
        not `None`.
      max_norm: If not `None`, embedding values are l2-normalized to this value.
      trainable: Whether or not the embedding is trainable. Default is True.
    
    
weighted_categorical_column    
      categorical_column: A `_CategoricalColumn` created by
        `categorical_column_with_*` functions.
      weight_feature_key: String key for weight values.
      dtype: Type of weights, such as `tf.float32`. Only float and integer weights
        are supported.
    
    
```
