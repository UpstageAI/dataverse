etl.cleaning
====================
Removing irrelevant, redun-dant, or noisy information from the data,
such as stop words or special characters.

etl.cleaning.char module
------------------------

.. automodule:: etl.cleaning.char
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: etl.cleaning.char.cleaning___char___remove_accent
.. autofunction:: etl.cleaning.char.cleaning___char___normalize_whitespace
.. autofunction:: etl.cleaning.char.cleaning___char___remove_unprintable


etl.cleaning.document module
----------------------------

.. automodule:: etl.cleaning.document
   :members:
   :undoc-members:
   :show-inheritance:


.. autofunction:: etl.cleaning.document.cleaning___document___split_by_word

etl.cleaning.html module
------------------------

.. automodule:: etl.cleaning.html
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: etl.cleaning.html.cleaning___html___extract_plain_text

etl.cleaning.korean module
--------------------------

.. automodule:: etl.cleaning.korean
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: etl.cleaning.korean.cleaning___korean___filter_by_ratio
.. autofunction:: etl.cleaning.korean.cleaning___korean___reduce_emoticon


etl.cleaning.length module
--------------------------

.. automodule:: etl.cleaning.length
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: etl.cleaning.length.cleaning___length___char_len_filter
.. autofunction:: etl.cleaning.length.cleaning___length___word_len_filter

etl.cleaning.number module
--------------------------

.. automodule:: etl.cleaning.number
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: etl.cleaning.number.cleaning___number___normalize

etl.cleaning.table module
-------------------------

.. automodule:: etl.cleaning.table
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: etl.cleaning.table.cleaning___table___merge_col_vertical

etl.cleaning.unicode module
---------------------------

.. automodule:: etl.cleaning.unicode
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: etl.cleaning.unicode.cleaning___unicode___remove_punct
.. autofunction:: etl.cleaning.unicode.cleaning___unicode___replace_punct
.. autofunction:: etl.cleaning.unicode.cleaning___unicode___normalize