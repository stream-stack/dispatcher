package back

import (
	"fmt"
	"regexp"
	"strings"
)

type Matcher struct {
	regexp  string
	data    interface{}
	compile *regexp.Regexp
}

func (p *Matcher) InitMatcher() error {
	compile, err := regexp.Compile("^" + p.regexp + "$")
	if err != nil {
		return err
	}
	p.compile = compile
	return nil
}

func (p *Matcher) Matcher(id string) bool {
	return p.compile.MatchString(id)
}

type TrieNode struct {
	Prefix       string //前缀
	Matchers     []*Matcher
	Children     map[string]*TrieNode
	IsEndingChar bool
}

var Root = &TrieNode{}

func AddNode(regexp string, data interface{}) error {
	split := strings.Split(regexp, "[")
	if len(split) != 2 {
		return fmt.Errorf("regexp format is 123[0-9]{5},not %s", regexp)
	}
	prefix := split[0]
	matcher := &Matcher{regexp: regexp, data: data}

	p := Root
	for i := 0; i < len(prefix); i++ {
		index := string(prefix[i])
		d := index
		if p.Children == nil {
			p.Children = make(map[string]*TrieNode)
		}
		if _, ok := p.Children[index]; !ok {
			newNode := &TrieNode{
				Prefix:   d,
				Matchers: []*Matcher{},
			}
			p.Children[index] = newNode
		}
		p = p.Children[index]
	}
	if err := matcher.InitMatcher(); err != nil {
		return err
	}
	p.Matchers = append(p.Matchers, matcher)
	p.IsEndingChar = true
	return nil
}

func Find(id string) (interface{}, bool) { //123  1[0-9]{2}
	p := Root
	for i := 0; i < len(id); i++ {
		index := string(id[i])
		if p.IsEndingChar && len(p.Matchers) > 0 {
			//匹配正则表达式
			for _, matcher := range p.Matchers {
				if matcher.Matcher(id) {
					return matcher.data, true
				}
			}
		}

		if _, ok := p.Children[index]; !ok {
			return nil, false
		}
		p = p.Children[index]
	}
	return nil, false
}
