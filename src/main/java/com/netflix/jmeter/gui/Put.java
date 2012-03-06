package com.netflix.jmeter.gui;

import java.awt.GridBagConstraints;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.sampler.AbstractSampler;
import com.netflix.jmeter.sampler.PutSampler;

public class Put extends AbstractGUI
{
    private static final long serialVersionUID = 3197090412869386190L;
    private static final String LABEL = "Cassandra Put";
    private JTextField CNAME;
    private JTextField VALUE;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;
    private JCheckBox IS_COUNTER;

    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        CNAME.setText(element.getPropertyAsString(PutSampler.COLUMN_NAME));
        VALUE.setText(element.getPropertyAsString(PutSampler.VALUE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(PutSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(PutSampler.VALUE_SERIALIZER_TYPE));
        IS_COUNTER.setSelected(element.getPropertyAsBoolean(PutSampler.IS_COUNTER));
    }

    public TestElement createTestElement()
    {
        PutSampler sampler = new PutSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");
        return sampler;
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);

        if (sampler instanceof PutSampler)
        {
            PutSampler gSampler = (PutSampler) sampler;
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            gSampler.setColumnName(CNAME.getText());
            gSampler.setValue(VALUE.getText());
            gSampler.setCounter(IS_COUNTER.isSelected());
        }
    }

    public void initFields()
    {
        CNAME.setText("${__Random(1,1000)}");
        VALUE.setText("${__Random(1,1000)}");
        CSERIALIZER.setSelectedItem("Column Serializer");
        VSERIALIZER.setSelectedItem("Value Serializer");
    }

    @Override
    public void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints)
    {
        addToPanel(mainPanel, labelConstraints, 0, 3, new JLabel("Column Name: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 3, CNAME = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 4, new JLabel("Column Value: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 4, VALUE = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 5, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 5, CSERIALIZER = new JComboBox(AbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 6, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 6, VSERIALIZER = new JComboBox(AbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, labelConstraints, 0, 7, new JLabel("Counter: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 7, IS_COUNTER = new JCheckBox());
    }

    @Override
    public String getLable()
    {
        return LABEL;
    }
}
