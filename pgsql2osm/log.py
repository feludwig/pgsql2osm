#!/usr/bin/python3

import typing
import os
import sys
import time

def n(i:int)->str :
    """ Format big numbers for easier readability
    """
    if i<1e3 :
        return str(i)
    elif i<1e6 :
        return f'{round(i/1e3,3):.3f}K'
    elif i<1e9 :
        return f'{round(i/1e6,6):.6f}M'
    elif i<1e12 :
        return f'{round(i/1e9,9):.9f}G'
    return f'{round(i/1e12,12):.12f}T'

class Logger() :
    def __init__(self) :
        self._ready=False
        self.previous_prependline=False
        self.previous_clearline=None
        #os.get_terminal_size() will error out when .isatty() is false.
        # instead of calling isatty() on each line, save it one at program start
        # (because it does not change)
        try :
            #sys.stderr.isatty() seems to return True when <prog>|tail -f /dev/stdin
            self.isatty=os.get_terminal_size().columns>0
        except OSError :
            self.isatty=False

    def check_ready(self) :
        assert self._ready, 'Need to run .set_phases first'
    def set_phases(self,phases:typing.Collection[str]) :
        self.phases=phases
        self.str_maxlen_phase=max(list(map(len,phases)))
        self.current_phase=0 #index into phases list
        self._ready=True

    def next_phase(self) :
        self.check_ready()
        self.current_phase+=1
        if self.current_phase>=len(self.phases) :
            self.current_phase=0
            l.log(f'WARNING: Called .next_phase() too many times with {self.phases}. resetting')

    def save_clearedline(self) :
        ''' Simply write a newline at the end of the previous clearline: save it.
        Warning, will garble output if not preceded by a clearline=True log call.
        '''
        print(end='\n',file=sys.stderr)
        sys.stderr.flush()
        #same behaviour whether followed by a clearline or not
        self.previous_clearline=False

    def log(self,*msg:typing.Any,clearline=False,prependline=False) :
        ''' Like print, accept a list of Any-typed items and print their .__str__()
        space-separated. Keeps track of the current phase.
        clearline==True will clear the line. Use when in a loop to display a counter going up
        prependline==True will not clear the line but leave the end without a newline.
            Use just before a .log(clearline=True) to add information before
        '''
        #self.check_ready() dont't for performance reasons
        assert int(clearline)+int(prependline)<2, 'not both clearline and prependline can be True'
        str_msg=' '.join(map(str,msg))
        l=self.str_maxlen_phase+5
        phase=str(self.current_phase+1)+'/'
        phase+=str(len(self.phases))
        phase+=' '+self.phases[self.current_phase]
        # a clearline will trigger clearing the line EXCEPT when the previous log was a prependline
        # -- OR --
        # two prependlines after eachother will trigger clearing the line: the newer erases the older
        should_flush=False
        if ((prependline or clearline) and not self.previous_prependline) or (self.previous_prependline and prependline):
            should_flush=True
            print(end='\r',file=sys.stderr)
        if not (clearline or prependline) and self.previous_clearline :
            should_flush=True
            print(file=sys.stderr) #reset clearline
        #a bit crazy syntax, but I want the field length to be variably dependent on self.str_maxlen_phase
        if not self.previous_prependline :
            # f-string will make -> '{:<13}'
            # str.format will do -> 'one          '
            str_msg=f'[ {{:<{l}}}] {{}}'.format(phase,str_msg)
        # \033[2K erasing the line makes a flicker, this should not
        # by just printing over with spaces, until end of terminal. it WILL get meesed up if you
        # resize the terminal while it's printing a progress...
        str_msg+=' '*((os.get_terminal_size().columns if self.isatty else 80)-len(str_msg))
        print(str_msg,end=('' if clearline else ' ' if prependline else '\n'),file=sys.stderr)
        self.previous_prependline=prependline
        self.previous_clearline=clearline
        if should_flush :
            sys.stderr.flush()

    def log_start(self,str_msg) :
        print('[ start ]',str_msg,file=sys.stderr)

class RateLogger(Logger) :
    """ Subclass of Logger because it reuses its .log() and various functions.
    Also a drop-in replacement to Logger but with additional .rate()
    Usage: normal like Logger, but 4 rate functions available: simplerate, rate, doublerate and
    triplerate. Calling one of those functions at every loop iteration with progress
    numbers will then print a progress bar with a custom message, and a rate (e.g 4.6M/s) of processed
    items.
    * NOTE: Need to call self.finishrate() at the end of the loop, to allow for other rate progress
    bars to be printed.
    * NOTE: calling self.log during a rate loop is also supported.
    """
    def __init__(self) :
        super().__init__()
        self.samples=[]
        self.times=[]
        self.sample_length=10_000
        self.min_time_interval_s=0.05
        self.prev_print_t=0
        self.prev_args=[]

    def check(self)->bool :
        """ Only calculate and print to console at time-distanced intervals.
        This is both because printing a lot will stress the stdout buffer (wasting CPU)
        and produce multiple megabytes of (useless) text output if piped to a file.
        Also calculating the average is not needed at every call of one of the rate functions.
        Use self.min_time_interval_s (50ms) as the minimal time between two calculate_avg+print
        steps.
        """
        t=time.time()
        if (t-self.prev_print_t)>self.min_time_interval_s :
            self.prev_print_t=t
            return True
        return False

    def samples_append(self,n:tuple) :
        """ The data in n is intended for a rate measurement: so also keep self.times
        updated. To prevent wasting memory, calculate a rolling average over the last
        self.sample_length (10'000) samples only. A total average would also be possible
        (and only require storing 1 value) but less expressive of "live"
        changes (Start some cpu-heavy background job-> the rate falls).
        """
        if len(self.samples)>=self.sample_length :
            self.samples.pop(0)
            self.times.pop(0)
        self.samples.append(n)
        self.times.append(time.time())

    def ratefmt(self,r:float) :
        ''' Return str(r) with 3 sigfigs
        '''
        for i,letter in ((1e12,'T'),(1e9,'G'),(1e6,'M'),(1e3,'K'),(1,'')) :
            if r>i or i==1: #give up at <1.0 point
                tgt=r/i
                # ljust for 3 -> 3.00
                if tgt<1.0 :
                    return str(round(tgt,3)).ljust(5,'0')+letter
                elif tgt<10.0 :
                    return str(round(tgt,2)).ljust(4,'0')+letter
                elif tgt<100.0 :
                    return str(round(tgt,1)).ljust(4,'0')+letter
                else :
                    return str(round(tgt)).ljust(3,'0')+letter
    
    def simplerate(self,count:int,msg:str,tot:int,lastline=False) :
        """ Show a rate progress bar on count from tot items in format:
            '{count} ({count_rate}/s) / {tot} {msg}    {percent:count/tot}%'
        """
        self.is_simplerate=True
        if count>1e6 :
            #set higher for a smoother rate display
            self.sample_length=100_000
        if not lastline :
            self.samples_append((count,))
        self.prev_args=(count,msg,tot)
        if self.check() or lastline :
            t_diff=self.times[-1]-self.times[0]
            if t_diff>1e-5 :
                #calculate avg
                r_a=(self.samples[-1][0]-self.samples[0][0])/t_diff
                r_s_a='('+self.ratefmt(r_a)+'/s)'
            else :
                r_s_a='(0/s)'
            l=(n(count),r_s_a+' / '+n(tot),msg,'   ',self.percent(count,tot),)
            self.log(*l,clearline=True)

    def rate(self,a:int,msg:str,count:int,total:int) :
        """ Show a rate progress bar on a for items from count to tot in format:
            '{a} ({a_rate}/s) {msg} {count} / {tot}   {percent:count/tot}%'
            This is useful when processing item count from total, producing a variable
            amount of sub-items, of which that other total is a (and total_a is unknowable)
        """
        self.multirate((a,),(msg,),count,total)

    def triplerate(self,a:int,a_msg:str,b:int,b_msg:str,c:int,c_msg:str,count:int,total:int) :
        """ Like .rate() ,show a rate progress bar on a for items from count to tot in format:
        But use three counts: a, b and c
            '{a} ({a_rate}/s) {a_msg} {b} ({b_rate}/s) {b_msg} {c} {c_rate/s} {c_msg} {count} / {tot}   {percent:count/tot}%'
        """
        self.multirate((a,b,c,),(a_msg,b_msg,c_msg,),count,total)

    def doublerate(self,a:int,a_msg:str,b:int,b_msg:str,count:int,total:int) :
        """ Like .rate() ,show a rate progress bar on a for items from count to tot in format:
        But use two counts: a and b
            '{a} ({a_rate}/s) {a_msg} {b} ({b_rate}/s) {b_msg} {count} / {tot}   {percent:count/tot}%'
        """
        self.multirate((a,b,),(a_msg,b_msg,),count,total)

    def multirate(self,ns:typing.Tuple[int],msgs:typing.Tuple[str],count:int,total:int,lastline=False) :
        """ Generalized version of .rate(), .doublerate() and .triplerate(). Those functions just
        reshuffle the arguments so that calling them has the args arranged in an order similar to
        how they will be printed out. Not .simplerate() though, it is separate.
        """
        self.is_simplerate=False
        if ns[0]>1e6 :
            self.sample_length=100_000
        if not lastline :
            self.samples_append(ns)
        self.prev_args=(ns,msgs,count,total)
        if self.check() or lastline:
            t_diff=self.times[-1]-self.times[0]
            if t_diff>1e-5 :
                #calculate avg
                rs=[(self.samples[-1][ix]-self.samples[0][ix])/t_diff for ix,_ in enumerate(ns)]
                r_ss=['('+self.ratefmt(rs[ix])+'/s)' for ix,_ in enumerate(ns)]
            else :
                r_ss=['(0/s)' for ix in enumerate(ns)]
            rates=[j for ix,i in enumerate(ns) for j in (n(i),r_ss[ix],msgs[ix])]
            l=(*rates,n(count)+' / '+n(total),'   ',self.percent(count,total),)
            self.log(*l,clearline=True)

    def finishrate(self,lastline=True) :
        """ Any currently running rate printer (simplerate,rate,doublerate,triplerate)
        has finished (for loop has ended) : reset counters and data storage.
        When lastline=False, do NOT calculate+print the final "summary 100%" line
        """
        if lastline :
            #last line print
            if self.is_simplerate :
                self.simplerate(*self.prev_args,lastline=True)
            else :
                self.multirate(*self.prev_args,lastline=True)
            self.save_clearedline()
        #reset rate measurement
        self.samples=[]
        self.times=[]
        self.prev_print_t=0
        self.prev_args=None
        self.sample_length=10_000

    def percent(self,numer:int,denom:int)->str :
        ''' Return the str(float(numer/denom)*100) with 3 sigfigs,
        '''
        r=numer/denom*100
        # ljust for 3.0 -> 3.00
        if r<1.0 :
            return str(round(r,3)).ljust(5,'0')+'%'
        elif r<10.0 :
            return str(round(r,2)).ljust(4,'0')+'%'
        else :
            return str(round(r,1)).ljust(3,'0')+'%'


l=RateLogger() #global variable
