'use strict';

var React = require('react');
var PureRenderMixin = require('./PureRenderMixin'); // deep-equals version of PRM
var DisclaimerModal = require('./DisclaimerModal');
var RawHTML = require('./RawHTML');
var {Modal, Button} = require('react-bootstrap');

var content = require('./content');
var Community = require('./Community');
var databaseKey = require('../databaseKey');
var {Navbar, Nav, DropdownButton} = require('react-bootstrap');
var VariantSearch = require('./VariantSearch');
var {Link} = require('react-router');

var NavLink = React.createClass({
    render: function () {
        var {children, ...otherProps} = this.props;
        return (
            <li>
                <Link {...otherProps} role='button'>
                    {children}
                </Link>
            </li>
        );
    }
});

var NavBarNew = React.createClass({
    close: function () {
        this.refs.about.setState({open: false});
    },
    getInitialState: function () {
        return {
            showModal: false,
        }
    },
    shouldComponentUpdate: function (nextProps, nextState) {
        // Only rerender if path has change or the research mode changes, ignoring query.
        var d3TipDiv = document.getElementsByClassName('d3-tip-selection');
        if (d3TipDiv.length != 0 && d3TipDiv[0].style.opacity != '0') {
            d3TipDiv[0].style.opacity='0';
            d3TipDiv[0].style.pointerEvents='none';
        }
        return this.props.mode !== nextProps.mode ||
            this.state.loggedin !== nextState.loggedin ||
            this.props.path.split(/\?/)[0] !== nextProps.path.split(/\?/)[0];
    },
    activePath: function (path, tab) {
        var navPath = (path === "") ? "home" : path.split("/")[0];
        return ((navPath === tab) ? "active" : "");
    },
    render: function () {
        var {path} = this.props;
        var brand = (
            <a className="navbar-brand" href="/">
                <h1>
                    <span className="BRCA">BRCA</span>
                    <span className="exchange"> Exchange</span>
                </h1>
                {this.props.mode === 'research_mode' && <span id="research-label" className="label label-info">Research</span>}
            </a>);
        return (
            <div className="navbar-container">
                <Navbar fixedTop brand={brand} toggleNavKey={0}>
                    <Nav eventKey={0} navbar right>
                        <NavLink to='/'>Home</NavLink>
                        <DropdownButton className={this.activePath(path, "about")} ref='about' title='About'>
                            <NavLink onClick={this.close} to='/about/variation'>
                                BRCA1, BRCA2, and Cancer
                            </NavLink>
                            <NavLink onClick={this.close} to='/about/history'>
                                History of the BRCA Exchange
                            </NavLink>
                            <NavLink onClick={this.close} to='/about/thisSite'>
                                This Site
                            </NavLink>
                        </DropdownButton>
                        <NavLink to='/variants'>Variants</NavLink>
                        <NavLink to='/help'>Help</NavLink>
                        <NavLink to='/community'>Community</NavLink>
                    </Nav>
                </Navbar>
            </div>
        );
    }
});


module.exports = {
    NavLink,
    NavBarNew
};
