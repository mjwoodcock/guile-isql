(use-modules (dbi dbi) (srfi srfi-1) (ice-9 rdelim))

; Save the stdout port for later use
(define stdout (current-output-port))
; dyndm-output-port is the file or stdout port currently used to write SQL results
(define dyndb-output-port stdout)
; dyndm-output-port is the filename currently used for writing SQL results
(define dyndb-output-filename "stdout")

; Should dyndb output CSV or table?
(define dyndb-output-csv #f)

(define *prompt1* "-> ")
(define *prompt2* "--> ")

(define db-handle #f)

(define *null-string* "<NULL>")
(define *blob-string* "<BLOB>")

; Generic exception handler.
(define (catch-all thunk)
  (catch #t
    thunk
    (lambda (key . parameters)
      (format (current-error-port)
              "Error: ~a\n" parameters)
      #f)))

; zip 2 lists together using a function
; Arguments:
;   f: function that takes 2 arguments
;   l1: list 1
;   l2: list 2
; Returns
;   a list
(define (zip-with f l1 l2)
  (if (or (null? l1) (null? l2))
    '()
    (cons (f (car l1) (car l2))
          (zip-with f (cdr l1) (cdr l2)))))

; Strip semicolon (and trailing space) from a string
(define (strip-semicolon s)
  (let ((idx (string-rindex s #\;)))
    (cond ((eq? idx #f) s)
          (else (string-take s idx)))))

(define (get-rows-accum db-obj lst)
  (let ((row (dbi-get_row db-obj)))
    (if (eq? row #f)
      lst
      (get-rows-accum db-obj (cons row lst)))))

(define (boolean->string v)
  (if v
    "true"
    "false"))

; get SQL result rows from database
(define (get-rows db-obj)
  (reverse (get-rows-accum db-obj '())))

; Get the display length of an SQL value
(define (val-length v)
  (cond ((null? v) (string-length *null-string*))
        ((boolean? v) 5)
        ((string? v) (string-length v))
        ((number? v) (string-length (number->string v)))
        ((u8vector? v) (string-length *blob-string*))
        (else 5)))

; Helper function for max-col-widths
(define (maxstr l1 l2)
  (zip-with
    (lambda (a b) (max a (val-length (cdr b))))
    l1
    l2))

; Find the maximum string length for all the columns in the database
(define (max-col-widths rows)
  (if (null? rows)
    '()
    (fold
      (lambda (lst acc) (maxstr acc lst))
      (make-list (length (car rows)) 0) ; FIXME: should this be lengths of col names?
      rows)))

; Convert a SQL value to a string, and pad to wdth chars
; Don't display BLOB data
(define (val-to-string-pad v wdth)
  (cond ((null? v) (string-pad-right *null-string* wdth))
        ((number? v) (string-pad-right (number->string v) wdth))
        ((string? v) (string-pad-right v wdth))
        ((boolean? v) (string-pad-right (boolean->string v) wdth))
        ((u8vector? v) (string-pad-right *blob-string* wdth))
        (else (string-pad v wdth))))

; Convert a SQL value to a string
; Don't display BLOB data
(define (val-to-string v)
  (cond ((null? v) *null-string*)
        ((number? v) (number->string v))
        ((string? v) (string-append "\"" (escape-string v) "\""))
        ((boolean? v) (boolean->string v))
        ((u8vector? v) *blob-string*)
        (else v)))

; Escape and quote chars in a string
(define (escape-string s)
  (string-join
    (string-split s (char-set #\"))
    "\\\""))

; Output the SQL table titles as CSV
(define (display-row-title-csv comma cols)
  (if (null? cols)
    (newline dyndb-output-port)
    (begin
      (display comma dyndb-output-port)
      (display (val-to-string (car (car cols))) dyndb-output-port)
      (display-row-title-csv "," (cdr cols)))))

(define (display-row-data-csv comma cols)
  (if (null? cols)
    (newline dyndb-output-port)
    (begin
      (display comma dyndb-output-port)
      (display (val-to-string (cdr (car cols))) dyndb-output-port)
      (display-row-data-csv "," (cdr cols)))))

; Output all SQL table rows
(define (display-rows-csv rows)
  (for-each
    (lambda (row) (display-row-data-csv "" row))
    rows))


; Output the SQL table titles
(define (display-row-title cols col-widths)
  (if (null? cols)
    (display "|\n" dyndb-output-port)
    (begin
      (display "|" dyndb-output-port)
      (display (val-to-string-pad (car (car cols)) (car col-widths)) dyndb-output-port)
      (display-row-title (cdr cols) (cdr col-widths)))))

; Output the SQL table separator
(define (display-row-separator col-widths)
  (if (null? col-widths)
    (display "|\n" dyndb-output-port)
    (begin
      (display "|" dyndb-output-port)
      (display (string-pad "" (car col-widths) #\-) dyndb-output-port)
      (display-row-separator (cdr col-widths)))))

; Output the SQL table data
(define (display-row-data cols col-widths)
  (if (null? cols)
    (display "|\n" dyndb-output-port)
    (begin
      (display "|" dyndb-output-port)
      (display (val-to-string-pad (cdr (car cols)) (car col-widths)) dyndb-output-port)
      (display-row-data (cdr cols) (cdr col-widths)))))

; Output all SQL table rows
(define (display-rows rows col-widths)
  (for-each
    (lambda (row) (display-row-data row col-widths))
    rows))

; Output SQL results
(define (results-to-string rows)
  (if (null? rows)
    (begin
      (display "No results" dyndb-output-port)(newline dyndb-output-port))
    (if (eq? dyndb-output-csv #t)
      (results-to-csv rows)
      (results-to-table rows (max-col-widths rows)))))

; Output SQL results as table
(define (results-to-table rows col-widths)
  (display-row-title (car rows) col-widths)
  (display-row-separator col-widths)
  (display-rows rows col-widths)
  (display-row-separator col-widths))

(define (results-to-csv rows)
  (display-row-title-csv "" (car rows))
  (display-rows-csv rows))

; Read user input
(define (prompt/read prompt)
  (display prompt)
  (read-line))

; Output SQL query result
(define (show-result cmd db-obj)
  (if (= (car (dbi-get_status db-obj)) 0)
    (case (string-contains cmd "select ")
      ((0)
        (let ((rows (get-rows db-obj)))
          (results-to-string rows)))
      (else
        (display "ok")))
    (display (string-append
               "ERROR: "
               (cdr (dbi-get_status db-obj))
               "\n"))))

; Check if string is an SQL statement
(define (is-stmt s)
  (if (eq? (string-ref (string-reverse (string-trim-both s)) 0) #\;)
    #t
    #f))

; Quit program
(define (cmd-quit args)
  (begin
    (if (not (eq? db-handle #f))
      (dbi-close db-handle))
    (display "Bye...")(newline)
    (quit)))

; Toggle output CSV flag
(define (cmd-toggle-csv args)
  (set! dyndb-output-csv (not dyndb-output-csv)))

; Show program help text
(define (cmd-help args)
  (display ":?            - this help")(newline)
  (display ":c            - toggle CVS or table output")(newline)
  (display ":o <fname>    - open filename for output")(newline)
  (display ":o            - output to stdout")(newline)
  (display ":q            - quit program")(newline)
  (display "connect <str> - connect to database")(newline)
  (display "<sql>;        - any valid SQL statement")(newline))


; Set SQL result output to either a file or stdout
(define (cmd-set-output args)
  (if (null? args)
    (begin
      ; Close old port if necessary
      (unless (eq? dyndb-output-port stdout)
        (close-port dyndb-output-port))
      (set! dyndb-output-port stdout)
      (set! dyndb-output-filename "stdout"))
    (let* ((filename (car args))
           (port (catch-all (lambda () (open-output-file filename)))))
      (unless (eq? port #f)
        (begin
          (set! dyndb-output-port port)
          (set! dyndb-output-filename filename))))))

; Show program settings
(define (cmd-settings args)
  (display "Output to: ")
  (display dyndb-output-filename)
  (newline)
  (display "CSV output: ")
  (display dyndb-output-csv)
  (newline))

; Command dispatch table
(define cmd-dispatch
  (list (cons ":?" cmd-help)
    (cons ":c" cmd-toggle-csv)
    (cons ":o" cmd-set-output)
    (cons ":q" cmd-quit)
    (cons ":s" cmd-settings)))

; Execute command
(define (do-cmd cmd)
  (when (> (string-length cmd) 1)
    (let* ((vals (string-split cmd #\space))
      (fn (assoc (car vals) cmd-dispatch)))
        (if (eq? fn #f)
          (display "Unknown command.  Enter ':?' for help\n")
          ((cdr fn) (cdr vals)))))

  (main-loop *prompt1* ""))

; Connect to database
(define (do-connect stmt)
  (let ((vals (string-split stmt #\space)))
    (if (not (= (length vals) 3))
      (begin
        (display "Invalid arguments: connect <driver> <connection string>")(newline))
      (let ((hnd (dbi-open (cadr vals) (caddr vals))))
        (if (= (car (dbi-get_status hnd)) 0)
          (set! db-handle hnd)
          (begin
            (display "ERROR:")
            (display (cdr (dbi-get_status hnd)))
            (newline)))))))

; Execute SQL statement
(define (do-sql stmt)
  (if (string-prefix? "connect " stmt)
    (do-connect stmt)
    (if (eq? db-handle #f)
      (begin
        (display "Not connected")(newline))
      (begin
        (dbi-query db-handle stmt)
        (show-result stmt db-handle)))))

(define (main-loop prompt old-cmd)
  (let ((cmd (prompt/read prompt)))
    (cond
      ((eof-object? cmd)
        (do-cmd ":q"))
      ((= (string-length cmd) 0)
        (main-loop *prompt1* ""))
      ((eq? (string-ref cmd 0) #\:)
        (do-cmd cmd))
      ((eq? (string-ref cmd 0) #\#) ; a # at the start of a line is a comment
        (main-loop prompt old-cmd))
      (else
        (if (is-stmt cmd)
          (let ((stmt (string-append old-cmd cmd)))
            (display stmt)(newline)
            (do-sql (strip-semicolon stmt))
            (main-loop *prompt1* ""))
          (main-loop *prompt2* (string-append cmd " ")))))))

(display "The db shell.  Enter ':?' for help")(newline)
(main-loop *prompt1* "")

